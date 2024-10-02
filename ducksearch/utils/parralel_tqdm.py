import tqdm
from joblib import Parallel


class ParallelTqdm(Parallel):
    """joblib.Parallel, but with a tqdm progressbar.

    Parameters
    ----------
    total : int
        The total number of tasks to complete.
    desc : str
        A description of the task.
    tqdm_bar : bool, optional
        Whether to display a tqdm progress bar. Default is False.
    show_joblib_header : bool, optional
        Whether to display the joblib header. Default is False

    References
    ----------
    https://github.com/joblib/joblib/issues/972
    """

    def __init__(
        self,
        *,
        total: int,
        desc: str,
        tqdm_bar: bool = True,
        show_joblib_header: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(verbose=(1 if show_joblib_header else 0), **kwargs)
        self.total = total
        self.desc = desc
        self.tqdm_bar = tqdm_bar
        self.progress_bar: tqdm.tqdm | None = None

    def __call__(self, iterable):
        try:
            return super().__call__(iterable)
        finally:
            if self.progress_bar is not None:
                self.progress_bar.close()

    __call__.__doc__ = Parallel.__call__.__doc__

    def dispatch_one_batch(self, iterator):
        """Dispatch a batch of tasks, and update the progress bar"""
        if self.progress_bar is None and self.tqdm_bar:
            self.progress_bar = tqdm.tqdm(
                desc=self.desc,
                total=self.total,
                position=0,
                disable=self.tqdm_bar,
                unit="tasks",
            )
        return super().dispatch_one_batch(iterator=iterator)

    dispatch_one_batch.__doc__ = Parallel.dispatch_one_batch.__doc__

    def print_progress(self):
        """Display the process of the parallel execution using tqdm"""
        if self.total is None and self._original_iterator is None:
            self.total = self.n_dispatched_tasks
            self.progress_bar.total = self.total
            self.progress_bar.refresh()

        if self.tqdm_bar:
            self.progress_bar.update(self.n_completed_tasks - self.progress_bar.n)
