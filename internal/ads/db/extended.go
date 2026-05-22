package db

// DB returns the underlying DBTX execution interface (e.g. pgxpool.Pool or pgx.Tx).
// Exposing this interface enables domain-level repositories to leverage active database transactions
// for operations requiring strict atomic boundaries, such as sync idempotency guarantees.
func (q *Queries) DB() DBTX {
	return q.db
}
