"""Stock huggingface_hub driver for test/hf_client_test.go.

Runs one snapshot_download per invocation with the UNMODIFIED system
huggingface_hub (tested pin: 0.36.2, Python 3.14). The Go caller provides a
fresh HF_HOME and a fresh local_dir so no Python-side cache can ever mask
what the mirror serves. This script is a driver only: all assertions live in
the Go test.

Usage:
    hf_client.py <endpoint> <repo_id> <revision> <local_dir> [expect-error]

Output (stdout, machine-parsable):
    DONE                              success
    EXPECTED_FAILURE <type>: <msg>    expect-error mode and it raised
    CAUSED_BY <type>: <msg>           chained cause (carries the HTTP 404)
    UNEXPECTED_SUCCESS                expect-error mode but it worked

Exit status: 0 on success or expected failure, 1 otherwise (traceback on
stderr for unexpected failures).
"""

import sys
import traceback

from huggingface_hub import snapshot_download


def main() -> int:
    endpoint, repo_id, revision, local_dir = sys.argv[1:5]
    expect_error = len(sys.argv) > 5 and sys.argv[5] == "expect-error"
    try:
        snapshot_download(
            repo_id=repo_id,
            endpoint=endpoint,
            local_dir=local_dir,
            revision=revision,
            token=False,  # anonymous and deterministic: no ambient token
        )
    except Exception as exc:  # reported here, asserted Go-side
        if not expect_error:
            traceback.print_exc()
            return 1
        cause = exc.__cause__ or exc
        print(f"EXPECTED_FAILURE {type(exc).__name__}: {exc}")
        print(f"CAUSED_BY {type(cause).__name__}: {cause}")
        return 0
    if expect_error:
        print("UNEXPECTED_SUCCESS")
        return 1
    print("DONE")
    return 0


if __name__ == "__main__":
    sys.exit(main())
