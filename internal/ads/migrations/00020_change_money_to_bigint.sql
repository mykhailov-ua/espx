-- +goose Up
-- +goose StatementBegin
ALTER TABLE campaigns 
    ALTER COLUMN budget_limit TYPE BIGINT USING (budget_limit * 1000000)::BIGINT,
    ALTER COLUMN current_spend TYPE BIGINT USING (current_spend * 1000000)::BIGINT,
    ALTER COLUMN daily_budget TYPE BIGINT USING (daily_budget * 1000000)::BIGINT;

ALTER TABLE customers 
    ALTER COLUMN balance TYPE BIGINT USING (balance * 1000000)::BIGINT,
    ALTER COLUMN allowed_overdraft TYPE BIGINT USING (allowed_overdraft * 1000000)::BIGINT;

ALTER TABLE balance_ledger 
    ALTER COLUMN amount TYPE BIGINT USING (amount * 1000000)::BIGINT;

ALTER TABLE customers DROP CONSTRAINT IF EXISTS chk_allowed_balance;
ALTER TABLE customers ADD CONSTRAINT chk_allowed_balance CHECK (balance + allowed_overdraft >= 0);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- +goose StatementEnd
