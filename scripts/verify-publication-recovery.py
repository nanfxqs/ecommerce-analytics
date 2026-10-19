"""Exercise a distinct validated revision, pre-switch failure, and successful retry.

Run on the dashboard host against a successfully published batch. The candidate
copies the export and adds an acceptance marker; analysis rows remain unchanged.
"""
import argparse
import json
import shutil
import uuid
from pathlib import Path
from retailrocket.publication import publish_batch, current_snapshot, BatchRun
from retailrocket.validation import validate_analysis
from retailrocket.query import overview, date_bounds


def counts(snapshot):
    bounds = date_bounds(snapshot.directory)
    if bounds[0] is None:
        return dict(view=0, addtocart=0, transaction=0)
    return overview(snapshot.directory/'events', *bounds)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('store', 'replay', 'export', 'candidate'):
        parser.add_argument(name, type=Path)
    args = parser.parse_args()
    before = current_snapshot(args.store)
    before_counts = counts(before)
    shutil.copytree(args.export, args.candidate)
    metadata_path = args.candidate/'metadata.json'
    metadata = json.loads(metadata_path.read_text())
    metadata['acceptance_attempt'] = uuid.uuid4().hex
    metadata_path.write_text(json.dumps(metadata, indent=2)+'\n')
    observed = {}

    def fail_before_switch(snapshot):
        observed['revision'] = snapshot.directory.name
        assert snapshot.directory != before.directory
        raise RuntimeError('controlled acceptance failure before pointer switch')

    try:
        publish_batch(args.store, args.replay, args.candidate,
                      validate=validate_analysis, before_switch=fail_before_switch)
        raise AssertionError('Fault injection did not execute')
    except RuntimeError as error:
        if str(error) != 'controlled acceptance failure before pointer switch':
            raise
    failed = json.loads(BatchRun(args.store, before.batch_id).path.read_text())
    assert failed['status'] == 'failed'
    preserved = current_snapshot(args.store)
    assert preserved.directory == before.directory
    assert counts(preserved) == before_counts
    after = publish_batch(args.store, args.replay, args.candidate, validate=validate_analysis)
    assert after.directory != before.directory
    assert after.directory.name == observed['revision']
    assert current_snapshot(args.store).directory == after.directory
    assert counts(after) == before_counts
    assert json.loads(BatchRun(args.store, before.batch_id).path.read_text())['status'] == 'published'
    print(json.dumps(dict(batch_id=before.batch_id, before_revision=before.directory.name,
                         candidate_revision=observed['revision'], revision_during_failure=preserved.directory.name,
                         after_retry_revision=after.directory.name, failed_status=failed['status'],
                         retry_status='published', before_counts=before_counts, after_counts=counts(after),
                         acceptance_attempt=metadata['acceptance_attempt']), indent=2))


if __name__ == '__main__':
    main()
