import functools
import logging
import multiprocessing
import tempfile
from typing import Optional

import rpy2.robjects as robjects
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


@functools.lru_cache(maxsize=None)
def warn_once() -> None:
    """Warn once about the dangers of RREPL."""
    logger.warning("R REPL can execute arbitrary code. Use with caution.")


class RREPL(BaseModel):
    """Simulates a standalone R REPL."""

    environment: Optional[Dict] = Field(default_factory=robjects.globalenv)

    @classmethod
    def worker(cls, command: str, environment, queue: multiprocessing.Queue) -> None:
        # Use a temporary file to capture R's print output
        with tempfile.NamedTemporaryFile(mode="w+", delete=True) as tempf_out, tempfile.NamedTemporaryFile(mode="w+", delete=True) as tempf_err:
            try:
                # Use R's sink function to redirect print output and error
                robjects.r(f'sink("{tempf_out.name}")')
                robjects.r(f'sink("{tempf_err.name}", type = "message")')

                # Define an error handler function that writes the error message to the error file
                error_handler = robjects.r('''
                function(err) {
                    sink(type = "message")
                    sink()
                    fileConn<-file("%s")
                    writeLines(showCondition(err), fileConn)
                    close(fileConn)
                }
                ''' % tempf_err.name)

                # Execute the command in the provided R environment
                res = robjects.r('tryCatch({0}, error={1})'.format(command, error_handler.r_repr()))

                # Stop redirecting print output
                robjects.r('sink(type = "message")')
                robjects.r('sink()')

                # Read the captured output from the temporary file
                tempf_out.seek(0)
                output = tempf_out.read()

                # Read the captured error from the temporary file
                tempf_err.seek(0)
                error = tempf_err.read()

                # Add the result, output, and error to the queue
                queue.put({'res': res, 'output': output, 'err': error})

            except Exception as e:
                queue.put({'err': str(e)})

    def run(self, command: str, timeout: Optional[int] = None) -> str:
        """Run command with own environment and returns anything printed.
        Timeout after the specified number of seconds."""

        # Warn against dangers of RREPL
        warn_once()

        queue: multiprocessing.Queue = multiprocessing.Queue()

        # Only use multiprocessing if we are enforcing a timeout
        if timeout is not None:
            # create a Process
            p = multiprocessing.Process(
                target=self.worker, args=(command, self.environment, queue)
            )

            # start it
            p.start()

            # wait for the process to finish or kill it after timeout seconds
            p.join(timeout)

            if p.is_alive():
                p.terminate()
                return "Execution timed out"
        else:
            self.worker(command, self.environment, queue)

        # get the result from the worker function
        return queue.get()
