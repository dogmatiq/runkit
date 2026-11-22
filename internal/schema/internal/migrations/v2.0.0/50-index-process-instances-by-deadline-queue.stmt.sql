CREATE INDEX CONCURRENTLY IF NOT EXISTS process_instances_by_deadline_queue
ON runkit.process_instances (handler_key, deadline_queue_id)
WHERE NOT ended;
