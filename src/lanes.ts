import type { ChannelKey } from './channel-key.js';

export interface JobInfo {
  kind: 'message' | 'change' | 'reaction' | 'other';
  id?: number;
  about?: number[];
}

export interface JobContext<W> {
  readonly w: W;
  readonly lane: Lane<W>;
  readonly ended: boolean;
  decide(id: number): void;
}

export type Job<W> = (ctx: JobContext<W>) => void | Promise<void>;

export interface ReadContext<W> {
  readonly lane: Lane<W>;
  readonly ended: boolean;
  readonly reading: boolean;
}

export type ReadJob<W> = (ctx: ReadContext<W>) => Job<W> | void | Promise<Job<W> | void>;

export interface Moments<W> {
  entry(w: W, key: ChannelKey, id: number, ended: boolean): void;
  overflow(w: W, lane: Lane<W>): void;
  start(w: W): void;
  end(w: W): void;
}

const aboutOnly = (info: JobInfo, ids: ReadonlySet<number>): boolean => info.about !== undefined && info.about.length > 0 && info.about.every((id) => ids.has(id));

type Entry<W> = { info: JobInfo; answer?: boolean } & ({ job: Job<W>; read?: undefined } | { read: ReadJob<W>; job?: undefined });

export class Lane<W> {
  private readonly hostQueue: Entry<W>[] = [];
  private readonly otherQueue: Entry<W>[] = [];
  private running = false;
  private holds = 0;
  private ended = false;
  private decidedThrough: number | undefined;
  readonly #w: W;

  constructor(
    readonly key: ChannelKey,
    private readonly lanes: Lanes<W>,
    w: W,
  ) {
    this.#w = w;
  }

  get place(): number | undefined {
    return this.decidedThrough;
  }

  #decide(id: number): void {
    if (Number.isFinite(id) && (this.decidedThrough === undefined || id > this.decidedThrough)) this.decidedThrough = id;
  }

  #context(): JobContext<W> {
    const lane = this;
    return {
      w: this.#w,
      lane,
      get ended() {
        return lane.ended;
      },
      decide: (id) => this.#decide(id),
    };
  }

  host(job: Job<W>): void {
    if (this.ended) return;
    this.hostQueue.push({ job, info: { kind: 'other' } });
    this.lanes.changed(this);
  }

  answer(job: Job<W>): void {
    if (this.ended) return this.#runAnswer(job);
    this.hostQueue.push({ job, info: { kind: 'other' }, answer: true });
    this.lanes.changed(this);
  }

  enter(job: Job<W>, info: JobInfo = { kind: 'other' }): void {
    if (info.kind === 'message' && info.id !== undefined) this.lanes.entered(this, info.id);
    if (this.ended) return;
    this.otherQueue.push({ job, info });
    if (info.kind === 'message') this.lanes.messageEntered(this);
    this.lanes.changed(this);
  }

  holdOtherUntil(until: Promise<unknown>): void {
    if (this.ended) return;
    this.holds++;
    const release = () => {
      this.holds--;
      this.lanes.changed(this);
    };
    until.then(release, release);
    this.lanes.changed(this);
  }

  enterFirst(job: Job<W>, info: JobInfo = { kind: 'other' }): void {
    if (info.kind === 'message' && info.id !== undefined) this.lanes.entered(this, info.id);
    if (this.ended) return;
    this.otherQueue.unshift({ job, info });
    if (info.kind === 'message') this.lanes.messageEntered(this);
    this.lanes.changed(this);
  }

  enterRead(read: ReadJob<W>): void {
    if (this.ended) return;
    this.otherQueue.push({ read, info: { kind: 'other' } });
    this.lanes.changed(this);
  }

  enterReadFirst(read: ReadJob<W>): void {
    if (this.ended) return;
    this.otherQueue.unshift({ read, info: { kind: 'other' } });
    this.lanes.changed(this);
  }

  waitingMessages(): number[] {
    return this.otherQueue.filter((e) => e.info.kind === 'message').map((e) => e.info.id ?? NaN);
  }

  replaceMessages(read: ReadJob<W>): number[] {
    const first = this.otherQueue.findIndex((e) => e.info.kind === 'message');
    if (first < 0) return [];
    const ids = this.waitingMessages();
    const given = new Set(ids);
    const aboutGiven = (e: Entry<W>) => e.info.kind === 'change' && aboutOnly(e.info, given);
    const kept = this.otherQueue.filter((e, i) => (i < first || e.info.kind !== 'message') && !aboutGiven(e));
    const at = this.otherQueue.slice(0, first).filter((e) => !aboutGiven(e)).length;
    kept.splice(at, 0, { read, info: { kind: 'other' } });
    this.otherQueue.length = 0;
    this.otherQueue.push(...kept);
    this.lanes.messagesLeft(ids.length);
    this.lanes.changed(this);
    return ids;
  }

  dropAbout(ids: readonly number[]): void {
    const out = new Set(ids);
    const kept = this.otherQueue.filter((e) => !aboutOnly(e.info, out));
    if (kept.length === this.otherQueue.length) return;
    this.otherQueue.length = 0;
    this.otherQueue.push(...kept);
    this.lanes.changed(this);
  }

  get isEnded(): boolean {
    return this.ended;
  }

  get isQuiet(): boolean {
    return !this.running && this.hostQueue.length === 0 && this.otherQueue.length === 0;
  }

  end(): void {
    this.ended = true;
    this.lanes.messagesLeft(this.waitingMessages().length);
    const answers = this.hostQueue.filter((e) => e.answer);
    this.hostQueue.length = 0;
    this.otherQueue.length = 0;
    for (const { job } of answers) if (job) this.#runAnswer(job);
  }

  #runAnswer(job: Job<W>): void {
    const failed = (err: unknown): void => {
      console.error(`[zulip-mcp] an answer on ${this.key} failed:`, (err as Error).message ?? err);
    };
    try {
      void Promise.resolve(job(this.#context())).catch(failed);
    } catch (err) {
      failed(err);
    }
  }

  pump(): Promise<void> | null {
    if (this.running || this.ended) return null;
    let entry = this.hostQueue.shift();
    if (!entry && this.holds === 0 && this.otherQueue.length > 0) {
      if (this.otherQueue[0].read && !this.lanes.takeTurn(this)) return null;
      entry = this.otherQueue.shift()!;
      if (entry.info.kind === 'message') this.lanes.messagesLeft(1);
    }
    if (!entry) return null;
    this.running = true;
    const run = entry;
    return (async () => {
      try {
        if (run.read) await this.#runRead(run.read);
        else await run.job(this.#context());
      } catch (err) {
        console.error(`[zulip-mcp] a delivery job on ${this.key} failed:`, (err as Error).message ?? err);
      } finally {
        this.running = false;
        this.lanes.changed(this);
      }
    })();
  }

  async #runRead(read: ReadJob<W>): Promise<void> {
    const lane = this;
    let reading = true;
    let hand: Job<W> | void;
    try {
      hand = await read({
        lane,
        get ended() {
          return lane.ended;
        },
        get reading() {
          return reading;
        },
      });
    } finally {
      reading = false;
      this.lanes.giveTurnBack(this);
    }
    if (hand) await hand(this.#context());
  }

  get canStartRead(): boolean {
    return !this.running && !this.ended && this.holds === 0 && this.hostQueue.length === 0 && this.otherQueue[0]?.read !== undefined;
  }
}

export interface LanesOptions {
  maxWaitingMessages: number;
}

export class Lanes<W> {
  private readonly lanes = new Map<ChannelKey, Lane<W>>();
  private readonly runningJobs = new Set<Promise<void>>();
  private turnHolder: Lane<W> | null = null;
  private readonly turnWaiters: Lane<W>[] = [];
  private readonly holdsAll = new Set<Promise<void>>();
  private waitingMessageCount = 0;
  private quietWaiters: (() => void)[] = [];
  private ended = false;
  readonly #w: W;
  readonly #moments: Moments<W>;

  constructor(w: W, moments: Moments<W>, private readonly options: LanesOptions) {
    this.#w = w;
    this.#moments = moments;
    moments.start(w);
  }

  lane(key: ChannelKey): Lane<W> {
    let lane = this.lanes.get(key);
    if (!lane) {
      lane = new Lane(key, this, this.#w);
      if (this.ended) lane.end();
      else for (const until of this.holdsAll) lane.holdOtherUntil(until);
      this.lanes.set(key, lane);
    }
    return lane;
  }

  holdAll(): () => void {
    let settle!: () => void;
    const until = new Promise<void>((resolve) => { settle = resolve; });
    this.holdsAll.add(until);
    for (const lane of this.lanes.values()) lane.holdOtherUntil(until);
    return () => {
      if (this.holdsAll.delete(until)) settle();
    };
  }

  keys(): IterableIterator<ChannelKey> {
    return this.lanes.keys();
  }

  get allHeld(): boolean {
    return this.holdsAll.size > 0;
  }

  quiet(): Promise<void> {
    if (this.isQuiet()) return Promise.resolve();
    return new Promise((resolve) => this.quietWaiters.push(resolve));
  }

  end(): void {
    if (this.ended) return;
    this.ended = true;
    for (const lane of this.lanes.values()) lane.end();
    this.wakeQuietWaiters();
  }

  async finish(graceMs: number, settle: () => Promise<void>): Promise<number> {
    this.end();
    let running = 0;
    if (this.runningJobs.size > 0) {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const grace = new Promise<void>((resolve) => { timer = setTimeout(resolve, graceMs); });
      await Promise.race([Promise.allSettled([...this.runningJobs]), grace]);
      clearTimeout(timer);
      running = this.runningJobs.size;
    }
    try {
      await settle();
    } finally {
      this.#moments.end(this.#w);
    }
    return running;
  }

  get waitingMessages(): number {
    return this.waitingMessageCount;
  }

  changed(lane: Lane<W>): void {
    queueMicrotask(() => {
      const started = lane.pump();
      if (started) {
        this.runningJobs.add(started);
        void started.finally(() => this.runningJobs.delete(started));
      }
      this.offerTurn();
      this.wakeQuietWaiters();
    });
  }

  entered(lane: Lane<W>, id: number): void {
    this.#moments.entry(this.#w, lane.key, id, lane.isEnded);
  }

  messageEntered(_lane: Lane<W>): void {
    this.waitingMessageCount++;
    if (this.waitingMessageCount <= this.options.maxWaitingMessages) return;
    let most: Lane<W> | null = null;
    let mostCount = 0;
    for (const lane of this.lanes.values()) {
      const n = lane.waitingMessages().length;
      if (n > mostCount) {
        most = lane;
        mostCount = n;
      }
    }
    if (most) this.#moments.overflow(this.#w, most);
  }

  messagesLeft(n: number): void {
    this.waitingMessageCount -= n;
  }

  takeTurn(lane: Lane<W>): boolean {
    const at = this.turnWaiters.indexOf(lane);
    const longer = at < 0 ? this.turnWaiters : this.turnWaiters.slice(0, at);
    if (this.turnHolder === null && !longer.some((other) => other.canStartRead)) {
      this.turnHolder = lane;
      if (at >= 0) this.turnWaiters.splice(at, 1);
      return true;
    }
    if (at < 0) this.turnWaiters.push(lane);
    return false;
  }

  giveTurnBack(lane: Lane<W>): void {
    if (this.turnHolder !== lane) return;
    this.turnHolder = null;
    this.offerTurn();
  }

  private offerTurn(): void {
    if (this.turnHolder !== null) return;
    const next = this.turnWaiters.find((other) => other.canStartRead);
    if (next) this.changed(next);
  }

  isQuiet(): boolean {
    for (const lane of this.lanes.values()) if (!lane.isEnded && !lane.isQuiet) return false;
    return true;
  }

  private wakeQuietWaiters(): void {
    if (this.quietWaiters.length === 0 || !this.isQuiet()) return;
    const waiters = this.quietWaiters;
    this.quietWaiters = [];
    for (const resolve of waiters) resolve();
  }
}
