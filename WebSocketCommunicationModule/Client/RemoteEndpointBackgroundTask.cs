namespace Jtext103.CFET2.WebSocketCommunicationModule.Client
{
    internal static class RemoteEndpointBackgroundTask
    {
        public static Task Observe(Task task)
        {
            if (task.IsCompleted)
            {
                return task;
            }

            return task.ContinueWith(
                completed =>
                {
                    _ = completed.Exception;
                },
                TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously);
        }
    }
}
