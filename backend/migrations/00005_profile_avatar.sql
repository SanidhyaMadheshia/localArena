-- +goose Up
-- Expand step of an expand/contract change (adr/0005): a new nullable column.
-- Code from before this migration keeps working because every query names its
-- columns explicitly; new code may read/write avatar_url. No default rewrite,
-- no lock beyond a brief ACCESS EXCLUSIVE for the catalog update.
ALTER TABLE profiles ADD COLUMN avatar_url text CHECK (avatar_url ~ '^https://');

-- +goose Down
ALTER TABLE profiles DROP COLUMN avatar_url;
