using System.Buffers;
using System.Net.WebSockets;
using System.Text;
using Jtext103.CFET2.WebSocketCommunicationModule.Protocol;

namespace TestCode;

[TestClass]
public sealed class WebSocketMessageReceiverFastPathTests
{
    [TestMethod]
    public async Task EmptyText_ShouldRespectZeroLimitAndReturnLease()
    {
        var pool = new PoisonPool();
        using var socket = Frames(([], WebSocketMessageType.Text, true));
        Assert.AreEqual(string.Empty, await Read(socket, pool, 0));
        Assert.AreEqual(1, pool.Returns);
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task SingleText_ShouldEnforceLimitBeforeReturning(bool exceedsLimit)
    {
        var pool = new PoisonPool();
        using var socket = Frames((Encoding.UTF8.GetBytes(exceedsLimit ? "abcd" : "abc"), WebSocketMessageType.Text, true),
            (Encoding.UTF8.GetBytes("ok"), WebSocketMessageType.Text, true));
        if (exceedsLimit) await Assert.ThrowsExactlyAsync<FormatException>(() => Read(socket, pool, 3));
        else Assert.AreEqual("abc", await Read(socket, pool, 3));
        Assert.AreEqual("ok", await Read(socket, pool, 3));
        Assert.AreEqual(2, pool.Returns);
    }

    [TestMethod]
    public async Task BinarySingleFrame_ShouldRejectAndKeepNextMessageReadable()
    {
        var pool = new PoisonPool();
        using var socket = Frames(([1, 2], WebSocketMessageType.Binary, true),
            (Encoding.UTF8.GetBytes("ok"), WebSocketMessageType.Text, true));
        await Assert.ThrowsExactlyAsync<FormatException>(() => Read(socket, pool, 64));
        Assert.AreEqual("ok", await Read(socket, pool, 64));
        Assert.AreEqual(2, pool.Returns);
    }

    [TestMethod]
    public async Task FragmentedUtf8_ShouldDecodeAcrossEveryByteBoundary()
    {
        const string expected = "A中🙂Z";
        var bytes = Encoding.UTF8.GetBytes(expected);
        for (var split = 1; split < bytes.Length; split++)
        {
            var pool = new PoisonPool();
            using var socket = Frames((bytes[..split], WebSocketMessageType.Text, false),
                (bytes[split..], WebSocketMessageType.Text, true));
            Assert.AreEqual(expected, await Read(socket, pool, bytes.Length), $"UTF8 split at byte {split}.");
            Assert.AreEqual(1, pool.Returns);
        }
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task MalformedUtf8_ShouldKeepExistingReplacementPolicy(bool fragmented)
    {
        byte[] bytes = [0x61, 0xe4, 0xb8, 0xff, 0x62];
        var pool = new PoisonPool();
        using var socket = fragmented
            ? Frames((bytes[..2], WebSocketMessageType.Text, false), (bytes[2..], WebSocketMessageType.Text, true))
            : Frames((bytes, WebSocketMessageType.Text, true));
        Assert.AreEqual(Encoding.UTF8.GetString(bytes), await Read(socket, pool, bytes.Length));
        Assert.AreEqual(1, pool.Returns);
    }

    [TestMethod]
    public async Task EmptyFragments_ShouldDecodeOnlyWrittenBytes()
    {
        var pool = new PoisonPool();
        using var socket = Frames(([], WebSocketMessageType.Text, false),
            (Encoding.UTF8.GetBytes("中"), WebSocketMessageType.Text, false), ([], WebSocketMessageType.Text, true));
        Assert.AreEqual("中", await Read(socket, pool, 3));
        Assert.AreEqual(1, pool.Returns);
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task RejectionAfterTextFragment_ShouldDrainWithoutContaminatingNextMessage(bool binary)
    {
        var pool = new PoisonPool();
        using var socket = Frames((Encoding.UTF8.GetBytes("abc"), WebSocketMessageType.Text, false),
            (Encoding.UTF8.GetBytes("de"), binary ? WebSocketMessageType.Binary : WebSocketMessageType.Text, false),
            (Encoding.UTF8.GetBytes("ignored"), WebSocketMessageType.Text, true),
            (Encoding.UTF8.GetBytes("ok"), WebSocketMessageType.Text, true));
        await Assert.ThrowsExactlyAsync<FormatException>(() => Read(socket, pool, 4));
        Assert.AreEqual("ok", await Read(socket, pool, 4));
        Assert.AreEqual(2, pool.Returns);
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task PendingFragment_ShouldRetainLeaseUntilCloseOrCancellationCompletes(bool close)
    {
        var completion = new TaskCompletionSource<WebSocketReceiveResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        var pool = new PoisonPool();
        using var cancellation = new CancellationTokenSource();
        using var socket = new TestSocket((buffer, _) => {
            if (calls++ != 0) return completion.Task;
            buffer.AsSpan()[0] = (byte)'a';
            return Task.FromResult(new WebSocketReceiveResult(1, WebSocketMessageType.Text, false));
        });
        var receive = WebSocketMessageReceiver.ReceiveTextAsync(socket, 32, 64, cancellation.Token, pool);
        cancellation.Cancel();
        Assert.IsFalse(receive.IsCompleted);
        Assert.AreEqual(0, pool.Returns);
        if (close)
        {
            completion.SetResult(new WebSocketReceiveResult(0, WebSocketMessageType.Close, true));
            await Assert.ThrowsAsync<WebSocketException>(() => receive);
        }
        else
        {
            completion.SetCanceled(cancellation.Token);
            await Assert.ThrowsAsync<OperationCanceledException>(() => receive);
        }
        Assert.AreEqual(1, pool.Returns);
    }

    [TestMethod]
    public void SingleSmallMessage_ShouldAvoidPerMessageAssemblyBuffers()
    {
        var bytes = Encoding.UTF8.GetBytes("{\"value\":1}");
        var result = Task.FromResult(new WebSocketReceiveResult(bytes.Length, WebSocketMessageType.Text, true));
        using var socket = new TestSocket((buffer, _) => { bytes.AsSpan().CopyTo(buffer.AsSpan()); return result; });
        var pool = new PoisonPool();
        void Consume()
        {
            var task = Read(socket, pool, 64);
            Assert.IsTrue(task.IsCompletedSuccessfully, "Allocation accounting must not cross threads.");
            Assert.AreEqual("{\"value\":1}", task.GetAwaiter().GetResult());
        }
        for (var i = 0; i < 64; i++) Consume();
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1024; i++) Consume();
        var perMessage = (GC.GetAllocatedBytesForCurrentThread() - before) / 1024;
        Assert.IsTrue(perMessage < 256, $"Unexpected small-message allocation: {perMessage} B/message.");
    }

    private static Task<string> Read(WebSocket socket, PoisonPool pool, int maximum) =>
        WebSocketMessageReceiver.ReceiveTextAsync(socket, 32, maximum, CancellationToken.None, pool);

    private static TestSocket Frames(params (byte[] bytes, WebSocketMessageType type, bool end)[] frames)
    {
        var index = 0;
        return new TestSocket((buffer, _) => {
            var frame = frames[index++];
            frame.bytes.AsSpan().CopyTo(buffer.AsSpan());
            return Task.FromResult(new WebSocketReceiveResult(frame.bytes.Length, frame.type, frame.end));
        });
    }

    private sealed class PoisonPool : ArrayPool<byte>
    {
        private readonly byte[] bytes = new byte[96];
        private bool leased;
        public int Returns { get; private set; }
        public override byte[] Rent(int minimumLength)
        {
            Assert.IsFalse(leased);
            Assert.IsTrue(minimumLength <= bytes.Length);
            leased = true;
            Array.Fill(bytes, (byte)0xcc);
            return bytes;
        }
        public override void Return(byte[] array, bool clearArray = false)
        {
            Assert.IsTrue(leased);
            Assert.AreSame(bytes, array);
            leased = false;
            Array.Fill(array, (byte)0xdd);
            Returns++;
        }
    }

    private sealed class TestSocket(Func<ArraySegment<byte>, CancellationToken, Task<WebSocketReceiveResult>> receive) : WebSocket
    {
        public override WebSocketCloseStatus? CloseStatus => null;
        public override string? CloseStatusDescription => null;
        public override string? SubProtocol => null;
        public override WebSocketState State => WebSocketState.Open;
        public override void Abort() { }
        public override void Dispose() { }
        public override Task CloseAsync(WebSocketCloseStatus status, string? description, CancellationToken token) => Task.CompletedTask;
        public override Task CloseOutputAsync(WebSocketCloseStatus status, string? description, CancellationToken token) => Task.CompletedTask;
        public override Task SendAsync(ArraySegment<byte> buffer, WebSocketMessageType type, bool end, CancellationToken token) => throw new NotSupportedException();
        public override Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken token) => receive(buffer, token);
    }
}
