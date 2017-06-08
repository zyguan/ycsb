CREATE TABLE t_record (
  id INTEGER PRIMARY KEY,
  timestamp INTEGER,
  kind VARCHAR(20),
  payload TEXT);

CREATE INDEX i_record_timestamp ON t_record (timestamp);
CREATE INDEX i_record_kind ON t_record (kind);