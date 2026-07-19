using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace ExplorerTabUtility.Helpers;

public sealed class StaTaskScheduler : TaskScheduler, IDisposable
{
    private readonly Thread _staThread;
    private readonly BlockingCollection<Task> _tasks = new();
    private int _disposeState;

    public StaTaskScheduler()
    {
        _staThread = new Thread(Run)
        {
            IsBackground = true,
            Name = "STA Thread"
        };
        _staThread.SetApartmentState(ApartmentState.STA);
        _staThread.Start();
    }

    private void Run()
    {
        foreach (var task in _tasks.GetConsumingEnumerable())
        {
            try
            {
                TryExecuteTask(task);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Exception in STA thread: {ex}");
            }
        }
    }

    // Called by the TPL to queue a task
    protected override void QueueTask(Task task)
    {
        _tasks.Add(task);
    }

    // (Optional) TPL calls this to see if we can run inline
    protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
    {
        // Since this scheduler only wants tasks on its dedicated thread, we *generally*
        // disallow inlining unless this is the STA thread itself.
        if (Thread.CurrentThread == _staThread)
        {
            return TryExecuteTask(task);
        }
        return false;
    }

    protected override IEnumerable<Task> GetScheduledTasks()
    {
        return _tasks.ToArray();
    }

    /// <summary>
    /// Stops accepting work, waits for the STA thread to finish, and releases the task queue.
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposeState, 1) != 0) return;

        _tasks.CompleteAdding();
        _staThread.Join();
        _tasks.Dispose();
    }
}
