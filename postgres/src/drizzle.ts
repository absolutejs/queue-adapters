// Driver-neutral entry point: never load either optional connection factory.
export { buildPostgresJobStore } from './store';
export { queueJobsTable, queueSchema } from './schema';
export type { QueueJobInsert, QueueJobRow } from './schema';
