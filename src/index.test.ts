import { EventEmitter } from "node:events";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { PersistentSocket } from "./index";

class MockSocket extends EventEmitter {
  connecting = false;
  destroyed = false;

  connect() {
    this.connecting = true;
    // Simulate async connection
    setTimeout(() => {
      this.connecting = false;
      this.emit("connect");
    }, 0);
    return this;
  }

  destroy(error?: Error) {
    this.destroyed = true;
    this.emit("close");
    return this;
  }

  end(callback?: () => void) {
    this.emit("end");
    callback?.();
    return this;
  }

  pause() {
    return this;
  }

  resume() {
    return this;
  }

  write(chunk: Buffer | string, callback?: (error?: Error | null) => void) {
    if (this.destroyed) {
      callback?.(new Error("Socket is destroyed"));
      return false;
    }

    process.nextTick(() => callback?.());
    return true;
  }
}

describe("PersistentSocket", () => {
  let mockSocket: MockSocket;
  let mediator: PersistentSocket;

  beforeEach(() => {
    mockSocket = new MockSocket();
    const createSocket = vi.fn(() => {
      mockSocket.connect();
      return mockSocket;
    });

    // @ts-ignore
    mediator = new PersistentSocket({ createSocket: createSocket });
  });

  afterEach(() => {
    mediator.destroy();
  });

  describe("Initial connection", () => {
    it("should establish connection when writing data", async () => {
      const connectSpy = vi.spyOn(mockSocket, "connect");

      await new Promise<void>((resolve) => {
        mediator.write("test", (error) => {
          expect(error).toBeNull();
          expect(connectSpy).toHaveBeenCalled();
          resolve();
        });
      });
    });
  });

  describe("Data handling", () => {
    it("should receive data from the socket", async () => {
      const testData = Buffer.from("test data");

      await new Promise<void>((resolve) => {
        // First ensure we're connected
        mediator.write("initial", async () => {
          // Now set up the data listener
          mediator.on("data", (data) => {
            expect(data).toEqual(testData);
            resolve();
          });

          // Emit the data
          mockSocket.emit("data", testData);
        });
      });
    });

    it("should send data to the socket", async () => {
      const testData = Buffer.from("test data");
      const writeSpy = vi.spyOn(mockSocket, "write");

      await new Promise<void>((resolve) => {
        // First ensure we're connected
        mediator.write(testData, () => resolve());
      });

      expect(writeSpy).toHaveBeenCalledWith(testData, expect.any(Function));
    });

    it("should work with different encoding types", async () => {
      const testData = "😀";
      const writeSpy = vi.spyOn(mockSocket, "write");

      await new Promise<void>((resolve) => {
        mediator.write(testData, "utf8", () => resolve());
      });

      expect(writeSpy).toHaveBeenCalledWith(
        Buffer.from(testData),
        expect.any(Function),
      );
    });

    it("should continue receiving data after reconnection", async () => {
      const dataEvents: string[] = [];

      await new Promise<void>((resolve) => {
        mediator.write("initial", () => resolve());
      });

      mediator.on("data", (data) => {
        dataEvents.push(data.toString());
      });

      // Emit first data event
      mockSocket.emit("data", Buffer.from("before disconnect"));

      // Simulate disconnect
      mockSocket.emit("close");

      // Write to trigger reconnection
      await new Promise<void>((resolve) => {
        mediator.write("trigger reconnect", () => resolve());
      });

      // Emit data on new connection
      mockSocket.emit("data", Buffer.from("after disconnect"));

      expect(dataEvents).toEqual(["before disconnect", "after disconnect"]);
    });

    it("should handle backpressure while receiving data from the socket", async () => {
      const originalPush: (typeof mediator)["push"] =
        mediator.push.bind(mediator);
      let pushCount = 0;

      // Override push to simulate backpressure after first chunk
      mediator.push = function (chunk: Buffer) {
        pushCount++;
        originalPush(chunk);

        // Simulate backpressure
        return pushCount === 1;
      };

      await new Promise<void>((resolve) => {
        const dataEvents: Buffer[] = [];
        mediator.write("initial", () => {
          mediator.on("data", (data) => {
            dataEvents.push(data);
            if (dataEvents.length === 2) {
              expect(dataEvents).toEqual([
                Buffer.from("first chunk"),
                Buffer.from("second chunk"),
              ]);
              resolve();
            }
          });

          // Emit multiple chunks
          mockSocket.emit("data", Buffer.from("first chunk"));
          mockSocket.emit("data", Buffer.from("second chunk"));
        });
      });
    });

    it("should pause the socket when backpressure occurs and resume when relieved", async () => {
      const pauseSpy = vi.spyOn(mockSocket, "pause");
      const resumeSpy = vi.spyOn(mockSocket, "resume");

      await new Promise<void>((resolve) => {
        mediator.write("initial", () => {
          // Override push to simulate backpressure
          mediator.push = vi.fn(() => false);

          mockSocket.emit("data", Buffer.from("test"));

          expect(pauseSpy).toHaveBeenCalled();

          // Simulate reading from the stream
          mediator.read(1);

          expect(resumeSpy).toHaveBeenCalled();
          resolve();
        });
      });
    });
  });

  describe("Reconnection", () => {
    it("should queue writes when disconnected and send them after reconnection", async () => {
      const testData = Buffer.from("test data");
      const writeSpy = vi.spyOn(mockSocket, "write");

      // Simulate disconnect
      mockSocket.emit("close");

      await new Promise<void>((resolve) => {
        // Write while disconnected
        mediator.write(testData, (error) => {
          expect(error).toBeNull();
          expect(writeSpy).toHaveBeenCalledWith(testData, expect.any(Function));
          resolve();
        });
      });
    });

    it("should not propagate close events", async () => {
      const closeSpy = vi.fn();
      mediator.on("close", closeSpy);

      mockSocket.emit("close");
      expect(closeSpy).toHaveBeenCalledTimes(0);
    });
  });

  describe("Error handling", () => {
    it("should forward socket errors", async () => {
      const testData = Buffer.from("test data");
      const testError = new Error("Test error");

      await new Promise<void>((resolve) => {
        mediator.on("error", (error) => {
          expect(error).toBe(testError);
          resolve();
        });

        mediator.write(testData, () => {
          mockSocket.emit("error", testError);
        });
      });
    });

    it("should handle write errors", async () => {
      mockSocket.write = vi.fn((_, callback) => {
        callback?.(new Error("Write error"));
        return false;
      });

      mediator.on("error", (error) => {
        expect(error).toBeDefined();
        expect(error?.message).toBe("Write error");
      });

      await new Promise<void>((resolve) => {
        mediator.write("test", (error) => {
          expect(error).toBeDefined();
          expect(error?.message).toBe("Write error");
          resolve();
        });
      });
    });
  });

  describe("Stream lifecycle", () => {
    it("should not reconnect after being destroyed", async () => {
      const createSocketSpy = vi.spyOn(mediator as any, "createSocket");

      await new Promise<void>((resolve) => {
        mediator.destroy();

        mediator.write("test", (error) => {
          expect(error).toBeDefined();
          expect((error as NodeJS.ErrnoException).code).toBe(
            "ERR_STREAM_DESTROYED",
          );
          expect(createSocketSpy).not.toHaveBeenCalled();
          resolve();
        });
      });
    });
  });
});
