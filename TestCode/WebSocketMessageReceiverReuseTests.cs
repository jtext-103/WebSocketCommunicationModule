using System.Buffers;
using System.Net.WebSockets;
using System.Text;
using Jtext103.CFET2.WebSocketCommunicationModule.Protocol;

namespace TestCode;

[TestClass]
public sealed class WebSocketMessageReceiverReuseTests
{
    [TestMethod]
    public async Task Receive_ShouldUseConfiguredSliceAndReturnIndependentText()
    {
        var pool = new TrackingPool();
        using var socket = TextSocket("hello");
        var text = await WebSocketMessageReceiver.ReceiveTextAsync(socket, 32, 128, CancellationToken.None, pool);
        Assert.AreEqual("hello", text);
        Assert.AreEqual(32, socket.LastReceiveSize);
        Assert.AreEqual(1, pool.Returns);
        Assert.AreEqual(0, pool.Outstanding);
        // Return overwrites the entire rented array, including the received bytes.
        Assert.AreEqual("hello", text);
    }

    [TestMethod]
    [DataRow(WebSocketMessageType.Text)]
    [DataRow(WebSocketMessageType.Binary)]
    public async Task RejectedFragments_ShouldDrainAndKeepNextMessageReadable(WebSocketMessageType type)
    {
        var frames = new Queue<(byte[] bytes, WebSocketMessageType type, bool end)>([
            (Encoding.UTF8.GetBytes("abc"), type, false),
            (Encoding.UTF8.GetBytes("def"), type, true),
            (Encoding.UTF8.GetBytes("ok"), WebSocketMessageType.Text, true),
        ]);
        using var socket = new TestSocket((buffer, _) =>
        {
            var frame = frames.Dequeue();
            frame.bytes.AsSpan().CopyTo(buffer.AsSpan());
            return Task.FromResult(new WebSocketReceiveResult(frame.bytes.Length, frame.type, frame.end));
        });
        var pool = new TrackingPool();
        await Assert.ThrowsExactlyAsync<FormatException>(() =>
            WebSocketMessageReceiver.ReceiveTextAsync(socket, 32, 4, CancellationToken.None, pool));
        Assert.AreEqual(1, frames.Count, "Rejected messages must be drained to their boundary.");
        Assert.AreEqual("ok", await WebSocketMessageReceiver.ReceiveTextAsync(socket, 32, 4, CancellationToken.None, pool));
        Assert.AreEqual(2, pool.Returns);
        Assert.AreEqual(0, pool.Outstanding);
    }

    [TestMethod]
    public async Task Cancellation_ShouldRetainLeaseUntilReceiveOperationCompletes()
    {
        var pending = new TaskCompletionSource<WebSocketReceiveResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var socket = new TestSocket((_, _) => pending.Task);
        using var cancellation = new CancellationTokenSource();
        var pool = new TrackingPool();
        var receive = WebSocketMessageReceiver.ReceiveTextAsync(socket, 32, 128, cancellation.Token, pool);
        cancellation.Cancel();
        Assert.IsFalse(receive.IsCompleted, "Cancellation request is not completion of the underlying receive.");
        Assert.AreEqual(0, pool.Returns);
        pending.SetCanceled(cancellation.Token);
        await Assert.ThrowsAsync<OperationCanceledException>(() => receive);
        Assert.AreEqual(1, pool.Returns);
        Assert.AreEqual(0, pool.Outstanding);
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task FailureOrClose_ShouldReturnLease(bool closeFrame)
    {
        using var socket = new TestSocket((_, _) => closeFrame
            ? Task.FromResult(new WebSocketReceiveResult(0, WebSocketMessageType.Close, true))
            : Task.FromException<WebSocketReceiveResult>(new WebSocketException("receive failed")));
        var pool = new TrackingPool();
        await Assert.ThrowsAsync<WebSocketException>(() =>
            WebSocketMessageReceiver.ReceiveTextAsync(socket, 32, 128, CancellationToken.None, pool));
        Assert.AreEqual(1, pool.Returns);
        Assert.AreEqual(0, pool.Outstanding);
    }

    [TestMethod]
    public async Task ConcurrentConnections_ShouldNotShareAnOutstandingBuffer()
    {
        var firstPending = new TaskCompletionSource<WebSocketReceiveResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondPending = new TaskCompletionSource<WebSocketReceiveResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        ArraySegment<byte> firstBuffer = default, secondBuffer = default;
        using var firstSocket = new TestSocket((buffer, _) => { firstBuffer = buffer; return firstPending.Task; });
        using var secondSocket = new TestSocket((buffer, _) => { secondBuffer = buffer; return secondPending.Task; });
        var pool = new TrackingPool();
        var first = WebSocketMessageReceiver.ReceiveTextAsync(firstSocket, 32, 128, CancellationToken.None, pool);
        var second = WebSocketMessageReceiver.ReceiveTextAsync(secondSocket, 32, 128, CancellationToken.None, pool);
        Assert.AreNotSame(firstBuffer.Array, secondBuffer.Array);
        Assert.AreEqual(2, pool.Outstanding);
        Encoding.UTF8.GetBytes("one").AsSpan().CopyTo(firstBuffer.AsSpan());
        Encoding.UTF8.GetBytes("two").AsSpan().CopyTo(secondBuffer.AsSpan());
        firstPending.SetResult(new WebSocketReceiveResult(3, WebSocketMessageType.Text, true));
        Assert.AreEqual("one", await first);
        secondPending.SetResult(new WebSocketReceiveResult(3, WebSocketMessageType.Text, true));
        Assert.AreEqual("two", await second);
        Assert.AreEqual(2, pool.Returns);
        Assert.AreEqual(0, pool.Outstanding);
    }

    [TestMethod]
    public void RepeatedSmallMessages_ShouldAvoidFixed64KiBAllocationAfterWarmup()
    {
        using var socket = TextSocket("{\"value\":1}");
        var pool = new TrackingPool();
        for (var i = 0; i < 32; i++) ReceiveSynchronously(socket, pool);
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1024; i++) ReceiveSynchronously(socket, pool);
        var bytesPerMessage = (GC.GetAllocatedBytesForCurrentThread() - before) / 1024;
        Assert.AreEqual(1, pool.CreatedArrays);
        Assert.IsTrue(bytesPerMessage < 16 * 1024, $"Fixed-size receive allocation remains: {bytesPerMessage} bytes/message.");
        Assert.AreEqual(0, pool.Outstanding);
    }

    private static void ReceiveSynchronously(TestSocket socket, TrackingPool pool)
    {
        var receive = WebSocketMessageReceiver.ReceiveTextAsync(socket, 64 * 1024, 128, CancellationToken.None, pool);
        Assert.IsTrue(receive.IsCompletedSuccessfully, "This allocation measurement requires a synchronous test socket.");
        Assert.AreEqual("{\"value\":1}", receive.GetAwaiter().GetResult());
    }

    private static TestSocket TextSocket(string text)
    {
        var bytes = Encoding.UTF8.GetBytes(text);
        var result = Task.FromResult(new WebSocketReceiveResult(bytes.Length, WebSocketMessageType.Text, true));
        return new TestSocket((buffer, _) => { bytes.AsSpan().CopyTo(buffer.AsSpan()); return result; });
    }

    private sealed class TrackingPool : ArrayPool<byte>
    {
        private readonly object sync = new();
        private readonly Stack<byte[]> available = new();
        private readonly HashSet<byte[]> leased = new();
        public int Returns { get; private set; }
        public int CreatedArrays { get; private set; }
        public int Outstanding { get { lock (sync) return leased.Count; } }
        public override byte[] Rent(int minimumLength)
        {
            lock (sync)
            {
                byte[] buffer;
                if (available.TryPop(out var reused) && reused.Length >= minimumLength) buffer = reused;
                else { buffer = new byte[minimumLength + 17]; CreatedArrays++; }
                Array.Fill(buffer, (byte)0xcc);
                Assert.IsTrue(leased.Add(buffer));
                return buffer;
            }
        }
        public override void Return(byte[] array, bool clearArray = false)
        {
            lock (sync)
            {
                Assert.IsTrue(leased.Remove(array), "Unknown or double-returned buffer.");
                Array.Fill(array, (byte)0xdd);
                available.Push(array);
                Returns++;
            }
        }
    }

    private sealed class TestSocket(Func<ArraySegment<byte>, CancellationToken, Task<WebSocketReceiveResult>> receive) : WebSocket
    {
        public int LastReceiveSize { get; private set; }
        public override WebSocketCloseStatus? CloseStatus => null;
        public override string? CloseStatusDescription => null;
        public override string? SubProtocol => null;
        public override WebSocketState State => WebSocketState.Open;
        public override void Abort() { }
        public override void Dispose() { }
        public override Task CloseAsync(WebSocketCloseStatus closeStatus, string? statusDescription, CancellationToken cancellationToken) => Task.CompletedTask;
        public override Task CloseOutputAsync(WebSocketCloseStatus closeStatus, string? statusDescription, CancellationToken cancellationToken) => Task.CompletedTask;
        public override Task SendAsync(ArraySegment<byte> buffer, WebSocketMessageType messageType, bool endOfMessage, CancellationToken cancellationToken) => throw new NotSupportedException();
        public override Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken)
        {
            LastReceiveSize = buffer.Count;
            return receive(buffer, cancellationToken);
        }
    }
}
