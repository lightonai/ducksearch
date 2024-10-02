# ParallelTqdm

joblib.Parallel, but with a tqdm progressbar.



## Parameters

- **total** (*int*)

    The total number of tasks to complete.

- **desc** (*str*)

    A description of the task.

- **tqdm_bar** (*bool*) – defaults to `True`

    Whether to display a tqdm progress bar. Default is False.

- **show_joblib_header** (*bool*) – defaults to `False`

    Whether to display the joblib header. Default is False

- **kwargs**




## Methods

???- note "__call__"

    Main function to dispatch parallel tasks.

    **Parameters**

    - **iterable**    
    
???- note "debug"

???- note "dispatch_next"

    Dispatch more data for parallel processing

    This method is meant to be called concurrently by the multiprocessing callback. We rely on the thread-safety of dispatch_one_batch to protect against concurrent consumption of the unprotected iterator.

    
???- note "dispatch_one_batch"

    Prefetch the tasks for the next batch and dispatch them.

    The effective size of the batch is computed here. If there are no more jobs to dispatch, return False, else return True.  The iterator consumption and dispatching is protected by the same lock so calling this function should be thread safe.

    **Parameters**

    - **iterator**    
    
???- note "format"

    Return the formatted representation of the object.

    **Parameters**

    - **obj**    
    - **indent**     – defaults to `0`    
    
???- note "info"

???- note "print_progress"

    Display the process of the parallel execution using tqdm

    
???- note "warn"

## References

https://github.com/joblib/joblib/issues/972

