import type { ChannelKey } from './channel-key.js';

/** What the lanes know of a job in the other queue. */
export interface JobInfo {
  /** A message counts against the cap on waiting messages; `id` is its Zulip id. */
  kind: 'message' | 'other';
  id?: number;
  /** The job reads from Zulip, and takes a turn: one such job runs at a time across all lanes. */
  turn?: boolean;
}

export interface JobContext<W> {
  /** What a job may write with. Only the lanes hand it out. */
  readonly w: W;
  /** True once the lane's connection has ended. A job that waited on Zulip then writes and sends nothing. */
  readonly ended: boolean;
}

export type Job<W> = (ctx: JobContext<W>) => void | Promise<void>;

interface Entry<W> {
  job: Job<W>;
  info: JobInfo;
}

/**
 * One channel's delivery work for one connection: one job at a time, the
 * host's queue before the other.
 */
export class Lane<W> {
  private readonly hostQueue: Entry<W>[] = [];
  private readonly otherQueue: Entry<W>[] = [];
  private running = false;
  private holds = 0;
  private waitingForTurn = false;
  private ended = false;

  constructor(
    readonly key: ChannelKey,
    private readonly lanes: Lanes<W>,
  ) {}

  /** What the host sent about this channel: taken before anything in the other queue. */
  host(job: Job<W>): void {
    if (this.ended) return;
    this.hostQueue.push({ job, info: { kind: 'other' } });
    this.lanes.changed(this);
  }

  /** Everything else, in the order it enters. */
  enter(job: Job<W>, info: JobInfo = { kind: 'other' }): void {
    if (this.ended) return;
    this.otherQueue.push({ job, info });
    if (info.kind === 'message') this.lanes.messageEntered(this);
    this.lanes.changed(this);
  }

  /** The other queue waits until `until` settles, however it settles. The host's queue runs meanwhile. */
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

  /** A job that goes ahead of everything waiting in the other queue, such as a read to try again. */
  enterFirst(job: Job<W>, info: JobInfo = { kind: 'other' }): void {
    if (this.ended) return;
    this.otherQueue.unshift({ job, info });
    if (info.kind === 'message') this.lanes.messageEntered(this);
    this.lanes.changed(this);
  }

  /** Messages waiting in the other queue, oldest first. */
  waitingMessages(): number[] {
    return this.otherQueue.filter((e) => e.info.kind === 'message').map((e) => e.info.id ?? NaN);
  }

  /**
   * The waiting messages leave the queue, and `job` takes the place of the
   * first of them. Returns their ids, oldest first.
   */
  replaceMessages(job: Job<W>, info: JobInfo): number[] {
    const first = this.otherQueue.findIndex((e) => e.info.kind === 'message');
    if (first < 0) return [];
    const ids = this.waitingMessages();
    const kept = this.otherQueue.filter((e, i) => i < first || e.info.kind !== 'message');
    kept.splice(first, 0, { job, info });
    this.otherQueue.length = 0;
    this.otherQueue.push(...kept);
    this.lanes.messagesLeft(ids.length);
    this.lanes.changed(this);
    return ids;
  }

  get isEnded(): boolean {
    return this.ended;
  }

  /** No job runs, waits, or is held back. */
  get isQuiet(): boolean {
    return !this.running && this.hostQueue.length === 0 && this.otherQueue.length === 0;
  }

  /** Called by the lanes: what waits goes, and a running job sees the end. */
  end(): void {
    this.ended = true;
    this.lanes.messagesLeft(this.waitingMessages().length);
    this.hostQueue.length = 0;
    this.otherQueue.length = 0;
  }

  /** Called by the lanes: start the next job, if this lane may. */
  pump(): Promise<void> | null {
    if (this.running || this.ended) return null;
    let entry = this.hostQueue.shift();
    if (!entry && this.holds === 0 && this.otherQueue.length > 0) {
      const next = this.otherQueue[0];
      if (next.info.turn && !this.lanes.takeTurn(this)) {
        this.waitingForTurn = true;
        return null;
      }
      this.waitingForTurn = false;
      entry = this.otherQueue.shift()!;
      if (entry.info.kind === 'message') this.lanes.messagesLeft(1);
    }
    if (!entry) return null;
    this.running = true;
    const lane = this;
    const ctx: JobContext<W> = {
      w: this.lanes.writer,
      get ended() {
        return lane.ended;
      },
    };
    const turn = entry.info.turn === true;
    return (async () => {
      try {
        await entry.job(ctx);
      } catch (err) {
        console.error(`[zulip-mcp] a delivery job on ${this.key} failed:`, (err as Error).message ?? err);
      } finally {
        this.running = false;
        if (turn) this.lanes.giveTurnBack(this);
        this.lanes.changed(this);
      }
    })();
  }

  get isWaitingForTurn(): boolean {
    return this.waitingForTurn;
  }
}

export interface LanesOptions<W> {
  /** At most this many messages wait in all lanes together. */
  maxWaitingMessages: number;
  /** Past the cap: the lane with the most waiting messages, for the server to give them up. */
  onOverflow(lane: Lane<W>): void;
}

/** Every lane of one connection, and what they share: the writer, the cap on waiting messages, and the turn at Zulip. */
export class Lanes<W> {
  private readonly lanes = new Map<ChannelKey, Lane<W>>();
  private readonly runningJobs = new Set<Promise<void>>();
  private turnHolder: Lane<W> | null = null;
  private waitingMessageCount = 0;
  private quietWaiters: (() => void)[] = [];
  private ended = false;

  constructor(
    readonly writer: W,
    private readonly options: LanesOptions<W>,
  ) {}

  /** The lane of a channel, made at its first use. After the end, a lane that is already ended. */
  lane(key: ChannelKey): Lane<W> {
    let lane = this.lanes.get(key);
    if (!lane) {
      lane = new Lane(key, this);
      if (this.ended) lane.end();
      this.lanes.set(key, lane);
    }
    return lane;
  }

  /** Settles once no job runs or waits in any lane. A job that a hold or the turn keeps back is waiting. */
  quiet(): Promise<void> {
    if (this.isQuiet()) return Promise.resolve();
    return new Promise((resolve) => this.quietWaiters.push(resolve));
  }

  /** Ends every lane. Settles when the jobs still running have ended, or at `graceMs`, whichever is first. */
  async end(graceMs: number): Promise<number> {
    this.ended = true;
    for (const lane of this.lanes.values()) lane.end();
    this.wakeQuietWaiters();
    if (this.runningJobs.size === 0) return 0;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const grace = new Promise<void>((resolve) => {
      timer = setTimeout(resolve, graceMs);
      (timer as { unref?: () => void }).unref?.();
    });
    await Promise.race([Promise.allSettled([...this.runningJobs]), grace]);
    clearTimeout(timer);
    return this.runningJobs.size;
  }

  get waitingMessages(): number {
    return this.waitingMessageCount;
  }

  /** Called by a lane when it may have a job to start, or has gone quiet. */
  changed(lane: Lane<W>): void {
    queueMicrotask(() => {
      const started = lane.pump();
      if (started) {
        this.runningJobs.add(started);
        void started.finally(() => this.runningJobs.delete(started));
      }
      this.wakeQuietWaiters();
    });
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
    if (most) this.options.onOverflow(most);
  }

  messagesLeft(n: number): void {
    this.waitingMessageCount -= n;
  }

  takeTurn(lane: Lane<W>): boolean {
    if (this.turnHolder !== null && this.turnHolder !== lane) return false;
    this.turnHolder = lane;
    return true;
  }

  giveTurnBack(lane: Lane<W>): void {
    if (this.turnHolder !== lane) return;
    this.turnHolder = null;
    for (const other of this.lanes.values()) if (other.isWaitingForTurn) this.changed(other);
  }

  /** No job runs or waits in any lane that hasn't ended. */
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
