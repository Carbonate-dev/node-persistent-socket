import { Socket } from "node:net";
import { Duplex, type DuplexOptions } from "node:stream";

interface PersistentSocketOptions extends DuplexOptions {
  createSocket: () => Socket;
}

class PersistentSocket extends Duplex {
  private connecting: boolean = false;
  private createSocket: () => Socket;
  private ended: boolean = false;
  private socket: Socket | null = null;
  private writeQueue: Array<
    [Buffer, BufferEncoding, (error?: Error | null) => void]
  > = [];

  constructor(options: PersistentSocketOptions) {
    super(options);
    this.createSocket = options.createSocket;
  }

  override _destroy(
    error: Error | null,
    callback: (error?: Error | null) => void,
  ): void {
    this.ended = true; // Mark as ended when explicitly destroyed
    if (this.socket) {
      this.socket.destroy(error || undefined);
    }

    callback(error);
  }

  override _final(callback: (error?: Error | null) => void): void {
    this.ended = true; // Mark as ended when the stream is ended
    if (this.socket) {
      this.socket.end(() => callback());
    } else {
      callback();
    }
  }

  override _read(size: number): void {
    if (this.socket) {
      this.socket.resume();
    }
  }

  override _write(
    chunk: Buffer,
    encoding: BufferEncoding,
    callback: (error?: Error | null) => void,
  ): void {
    if (this.ended) {
      callback(new Error("Stream has been ended"));
      return;
    }

    if (this.socket?.destroyed === false) {
      this.socket.write(chunk, encoding, callback);
    } else {
      // Queue the write and attempt to reconnect
      this.writeQueue.push([chunk, encoding, callback]);
      this.ensureConnection().catch((error) => {
        callback(error);
      });
    }
  }

  private cleanupSocket() {
    this.socket?.removeAllListeners();
    this.socket = null;
  }

  private async ensureConnection(): Promise<void> {
    if (this.socket) return;
    if (this.connecting) return;
    if (this.ended) return; // Don't reconnect if we're actually ended

    this.connecting = true;

    try {
      const socket = this.createSocket();

      return new Promise((resolve, reject) => {
        const onError = (error: Error) => {
          this.connecting = false;
          reject(error);
        };

        socket.once("error", onError);

        socket.once("connect", () => {
          socket.removeListener("error", onError);
          this.setupSocket(socket);
          this.connecting = false;
          resolve();
        });
      });
    } catch (error) {
      this.connecting = false;
      throw error;
    }
  }

  private setupSocket(socket: Socket) {
    this.socket = socket;
    this.ended = false; // Reset the ended state when we get a new socket

    socket.on("data", (data) => {
      if (!this.push(data)) {
        socket.pause();
      }
    });

    socket.on("end", () => {
      if (this.socket === socket) {
        this.cleanupSocket();
        this.emit("disconnect");
      }
    });

    socket.on("error", (error) => {
      if (this.socket === socket) {
        this.emit("error", error);
        // this.emit('error', error);
        this.cleanupSocket();
      }
    });

    socket.on("close", () => {
      if (this.socket === socket) {
        this.cleanupSocket();
        this.emit("disconnect");
      }
    });

    // Process any pending writes
    while (this.writeQueue.length > 0) {
      const [chunk, encoding, callback] = this.writeQueue.shift()!;

      if (encoding && (encoding as "buffer" | BufferEncoding) !== "buffer") {
        this.socket.write(chunk, encoding, callback);
      } else {
        this.socket.write(chunk, callback);
      }
    }
  }
}

export { PersistentSocket };
export type { PersistentSocketOptions };
