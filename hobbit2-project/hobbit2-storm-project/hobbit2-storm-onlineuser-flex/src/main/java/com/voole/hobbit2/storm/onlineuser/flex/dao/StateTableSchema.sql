CREATE TABLE sp_online_state (
  stamp BIGINT NOT NULL, spid VARCHAR, total BIGINT, low BIGINT, CONSTRAINT spid_online_state_pk PRIMARY KEY (stamp, spid)
);
CREATE TABLE sp_online_state_snapshot (
   spid VARCHAR NOT NULL PRIMARY KEY, total BIGINT, low BIGINT
);
CREATE TABLE oem_online_state (
  stamp BIGINT NOT NULL, spid VARCHAR, oemid BIGINT NOT NULL, total BIGINT, low BIGINT, CONSTRAINT oem_online_state_pk PRIMARY KEY (stamp, oemid)
);
CREATE TABLE oem_online_state_snapshot (
  oemid BIGINT NOT NULL PRIMARY KEY,spid VARCHAR, total BIGINT, low BIGINT
);