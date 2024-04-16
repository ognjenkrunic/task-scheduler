using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Zadatak1
{
    public class CustomTaskScheduler : TaskScheduler
    {
        public readonly static int defaultPriority = 10;
        public readonly static int maxPriority = 1;
        private readonly static int maxUserPriority = 2;
        public readonly static int defaultDuration = 5;

        /// <summary>
        /// Funkcija odnosno zadatak koji zelimo da izvrsavamo.
        /// </summary>
        /// <param name="taskWrapper"></param>
        public delegate void SchedulableTask(TaskWrapper taskWrapper);
        /// <summary>
        /// Lista zadataka koje treba rasporediti za izvrsavanje.
        /// </summary>
        readonly List<TaskWrapper> pendingTasks = new List<TaskWrapper>();
        /// <summary>
        /// Niz zadataka koji su rasporedjeni i izvrsavaju se.
        /// </summary>
        readonly TaskWrapper[] executingTasks;
        /// <summary>
        /// Svojstvo koje sluzi za odredjivanje da li je rasporedjivanje preventivno ili nepreventivno.
        /// </summary>
        public bool IsPreemptive { get; set; }
        public int MaxParallelTasks => executingTasks.Length;
        public int CurrentTaskCount => executingTasks.Count(x => x != null);
        public int LeftTaskCount => pendingTasks.Count;
      
        readonly Dictionary<Task, TaskWrapper> taskRegister = new Dictionary<Task, TaskWrapper>();

        /// <summary>
        /// Konstruktor klase CustomTaskScheduler.
        /// </summary>
        /// <param name="maxNumOfParallelTasks">Podatak tipa int koji predstavlja broj niti na kojima se mogu izvrsavati zadaci.</param>
        /// /// <param name="isPreemptive">Podatak tipa bool koji odredjuje da li se radi o preventivnom ili nepreventivnom rasporedjivanju.</param>
        public CustomTaskScheduler(int maxNumOfParallelTasks, bool isPreemptive)
        {
            if (maxNumOfParallelTasks < 1)
                throw new ArgumentException("the number of parallel tasks can not be less than 1");

            executingTasks = new TaskWrapper[maxNumOfParallelTasks];
            IsPreemptive = isPreemptive;
        }

        /// <summary>
        /// Klasa koja sadrzi stvarni zadatak koji se izvrsava kao i ostale informacije o zadatku potrebne za ispravan rad rasporedjivaca.
        /// </summary>
        public class TaskWrapper
        {
            private int priority;

            private readonly object lockObj = new object();
            public int Priority
            {
                get
                {
                    lock (lockObj)
                    {
                        return priority;
                    }
                }
                set
                {
                    lock (lockObj)
                    {
                        priority = value;
                    }
                }
            }
            public int MaxDuration { get; private set; }
            /// <summary>
            /// Svojstvo koje govori da li je zadatak pauziran pri preventivnom rasporedjivanju.
            /// </summary>
            public bool IsPaused { get; set; }
            /// <summary>
            /// Svojstvo koje govori da li je zadatak zavrsio sa izvrsavanjem.
            /// </summary>
            public bool IsCancelled { get; private set; }
            /// <summary>
            /// Zadatak koji se stvarno izvrsava.
            /// </summary>
            public Task ExecutingTask { get; set; }
            /// <summary>
            /// Task namijenjen za kotrolu izvrsavanja. Ukoliko se ExecutingTask izvrsava duze nego je dozvoljeno, isti ce biti prekinut;
            /// </summary>
            public Task TaskWithCallback { get; set; }
            /// <summary>
            /// Token za otkazivanje taska.
            /// </summary>
            public CancellationTokenSource TokenSource { get; private set; }
            /// <summary>
            /// Vrijeme izvrsavanja zadatka.
            /// </summary>
            public Stopwatch ExecutionTime { get; }
            public EventWaitHandle Handler { get; private set; }

            public void Cancel() => IsCancelled = true;

            /// <summary>
            /// Konstruktor klase .
            /// </summary>
            /// <param name="schedulableTask"> Funkcija koja se izvrsava.</param>
            /// /// <param name="priority">Prioritet zadatka, pri cemu manja vrijednost predstavlja veci prioritet.</param>
            /// <param name="maxDuration">Maksimalno trajanje zadatka.</param>
            public TaskWrapper(SchedulableTask schedulableTask, int priority, int maxDuration)
            {
                if (priority < 1 || maxDuration < 1)
                    throw new ArgumentException("the value of priority and duration can not be less than 1");
                Priority = (priority == maxPriority) ? maxUserPriority : priority;
                MaxDuration = maxDuration;
                IsPaused = false;
                IsCancelled = false;
                ExecutingTask = new Task(() => schedulableTask(this), TokenSource.Token);
                TokenSource = new CancellationTokenSource();
                ExecutionTime = new Stopwatch();
                Handler = new EventWaitHandle(false, EventResetMode.AutoReset);
            }
          
            public override string ToString()
            {
                return "Task[Id: " + ExecutingTask.Id + " prioritet: " + Priority + "  trajanje: " + MaxDuration + "]";
            }
        }

        /// <summary>
        /// Funkcija koja dodaje novi zadatak u listu zadataka za rasporedjivanje i poziva funkciju za nepreventivno rasporedjivanje.
        /// </summary>
        /// <param name="schedulableTask">Funkcija koja se izvrsava.</param>
        /// <param name="maxDuration">Maksimalno trajanje zadatka.</param>
        /// <param name="priority">Prioritet zadatka.</param>
        public void ScheduleTaskNonPreemptive(SchedulableTask schedulableTask, int priority, int maxDuration)
        {
            if (!IsPreemptive)
            {
                TaskWrapper taskForExecution = new TaskWrapper(schedulableTask, priority, maxDuration);
                pendingTasks.Add(taskForExecution);
                SchedulePendingTasksNonPreemptive();
            }
        }

        private readonly object lockObj = new object();
        /// <summary>
        /// Funkcija za nepreventivno rasporedjivanje zadataka.
        /// </summary>
        public void SchedulePendingTasksNonPreemptive()
        {
            lock (lockObj)
            {
                AbortTasksOverQuota();
                ScheduleTasksOnAvailableThreadsNonPreemptive();
            }
        }

        /// <summary>
        /// Funkcija za zaustavljanje svih zadataka koji su prekoracili svoje maksimalno vrijeme izvrsavanja.
        /// </summary>
        private void AbortTasksOverQuota()
        {
            for (int i = 0; i < MaxParallelTasks; ++i)
            {
                if (executingTasks[i] != null)
                {
                    TaskWrapper task = executingTasks[i];
                    if (task.TokenSource.IsCancellationRequested || task.ExecutingTask.IsCanceled || task.ExecutingTask.IsCompleted)
                    {
                        executingTasks[i] = null;
                        task.ExecutionTime.Stop();
                    }
                }
            }
        }

        /// <summary>
        /// Nepreventivo rasporedjivanje zadataka na slobodne niti na osnovu njihovog prioriteta.
        /// </summary>
        private void ScheduleTasksOnAvailableThreadsNonPreemptive()
        {
            int[] availableThreads = executingTasks.Select((value, i) => (value, i)).Where(x => x.value == null).Select(x => x.i).ToArray();
            foreach (int freeThread in availableThreads)
            {
                if (LeftTaskCount != 0)
                {
                    TaskWrapper taskForExecution = pendingTasks.OrderBy(t => t.Priority).First();
                    pendingTasks.Remove(taskForExecution);
                    taskForExecution.ExecutingTask.Start();
                    taskForExecution.ExecutionTime.Start();
                    taskForExecution.TaskWithCallback = Task.Factory.StartNew(() =>
                    {
                        long duration = taskForExecution.MaxDuration * 1000;
                        long timeLeft;
                        while ((timeLeft = duration - taskForExecution.ExecutionTime.ElapsedMilliseconds) > 0)
                            Task.Delay((int)timeLeft).Wait();
                        taskForExecution.TokenSource.Cancel();
                        taskForExecution.Cancel();
                        taskForExecution.ExecutingTask.Wait();
                        SchedulePendingTasksNonPreemptive();

                    });
                    executingTasks[freeThread] = taskForExecution;
                }
            }

        }

        /// <summary>
        /// Funkcija koja dodaje zadatak u listu zadataka za izvrsavanje i poziva funkciju za preventivno rasporedjivanje.
        /// </summary>
        /// <param name="schedulableTask">Funkcija koja se izvrsava.</param>
        /// <param name="priority">Prioritet zadatka.</param>
        /// <param name="maxDuration">Trajanje zadatka.</param>
        public void ScheduleTaskPreemptive(SchedulableTask schedulableTask, int priority, int maxDuration)
        {
            if (IsPreemptive)
            {
                TaskWrapper taskForExecution = new TaskWrapper(schedulableTask, priority, maxDuration);
                pendingTasks.Add(taskForExecution);
                SwitchContextIfNeeded(taskForExecution.Priority);
                SchedulePendingTasksPreemptive();
            }
        }

        /// <summary>
        /// Funkcija za pronalazak slobodne niti ili niti sa zadatkom koji ima manji prioritet od prioriteta prosljedjenog zadatka.
        /// </summary>
        private void SwitchContextIfNeeded(int taskPriority)
        {
            int threadId = -1;
            int[] avaiableThreads = executingTasks.Select((value, i) => (value, i)).Where(t => t.value == null).Select(t => t.i).ToArray();
            if (avaiableThreads.Length > 0)
                threadId = avaiableThreads[0];
            else
            {
                (TaskWrapper task, int i) lowestPriorityTask = executingTasks.Select((value, i) => (value, i)).OrderByDescending(t => t.value.Priority).First();
                if (lowestPriorityTask.task.Priority > taskPriority)
                    threadId = lowestPriorityTask.i;
            }
            if (threadId != -1)
            {
                TaskWrapper executingTask = executingTasks[threadId];
                if (executingTask != null)
                {
                    executingTask.ExecutionTime.Stop();
                    executingTask.IsPaused = true;
                    executingTasks[threadId] = null;
                    pendingTasks.Add(executingTask);
                }
            }
        }

        private void SchedulePendingTasksPreemptive()
        {
            lock (lockObj)
            {
                AbortTasksOverQuota();
                ScheduleTasksOnAvailableThreadsPreemptive();
            }
        }

        /// <summary>
        /// Funkcija za preventivno rasporedjivanje zadataka na slobodne niti. Postoje dva slucaja: pokrenut prvi put ili pokrenut nakon pauze.
        /// </summary>
        private void ScheduleTasksOnAvailableThreadsPreemptive()
        {
            int[] availableThreads = executingTasks.Select((value, i) => (value, i)).Where(x => x.value == null).Select(x => x.i).ToArray();
            foreach (int freeThread in availableThreads)
            {

                if (LeftTaskCount != 0)
                {
                    TaskWrapper taskForExecution = pendingTasks.OrderBy(t => t.Priority).First();
                    pendingTasks.Remove(taskForExecution);
                    if (taskForExecution.IsPaused)
                    {
                        taskForExecution.IsPaused = false;
                        executingTasks[freeThread] = taskForExecution;
                        taskForExecution.ExecutionTime.Start();
                        taskForExecution.Handler.Set();
                    }
                    else
                    {
                        taskForExecution.ExecutingTask.Start();
                        taskForExecution.ExecutionTime.Start();
                        taskForExecution.TaskWithCallback = Task.Factory.StartNew(() =>
                        {
                            long duration = taskForExecution.MaxDuration * 1000;
                            long timeLeft;
                            while ((timeLeft = duration - taskForExecution.ExecutionTime.ElapsedMilliseconds) > 0)
                                Task.Delay((int)timeLeft).Wait();
                            taskForExecution.TokenSource.Cancel();
                            taskForExecution.Cancel();
                            taskForExecution.ExecutingTask.Wait();
                            SchedulePendingTasksPreemptive();

                        });
                        executingTasks[freeThread] = taskForExecution;
                    }
                }
            }

        }

        /// <summary>
        /// Funkcija za registrovanje zadataka.
        /// </summary>
        /// <param name="schedulableTask"></param>
        /// <param name="priority"></param>
        /// <param name="maxDuration"></param>
        /// <returns></returns>
        public Task RegisterTask(SchedulableTask schedulableTask, int priority, int maxDuration)
        {
            TaskWrapper task = new TaskWrapper(schedulableTask, priority, maxDuration);
            taskRegister.Add(task.ExecutingTask, task);
            return task.ExecutingTask;
        }

        protected override IEnumerable<Task> GetScheduledTasks() => pendingTasks.Select(t => t.ExecutingTask).ToArray().Union(executingTasks.Select(t => t.ExecutingTask));

        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
        {
            return false;
        }

        protected override void QueueTask(Task task)
        {
            if (taskRegister.ContainsKey(task))
            {
                pendingTasks.Add(taskRegister[task]);
                SchedulePendingTasksNonPreemptive();
            }
        }
    }
}
