CREATE TABLE local_candidate (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    ballot_number INTEGER NOT NULL,
    name VARCHAR(256) NOT NULL,
    sex CHAR(1) NOT NULL, -- M or F
    position VARCHAR(100) NOT NULL,
    partylist VARCHAR(255) DEFAULT NULL,
    lgu VARCHAR(255) DEFAULT NULL,
    district INTEGER DEFAULT NULL,
    province VARCHAR(255)
);

CREATE TABLE senator (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    ballot_number INTEGER NOT NULL,
    name VARCHAR(256) NOT NULL,
    sex CHAR(1) NOT NULL, -- M or F
    position VARCHAR(100) NOT NULL,
    partylist VARCHAR(255) DEFAULT NULL
);

CREATE TABLE partylist (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    ballot_number INTEGER NOT NULL,
    name VARCHAR(256) NOT NULL
);

CREATE TABLE province_summary (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    total_legislative_district INT NOT NULL DEFAULT 0,
    total_provincial_district INT NOT NULL DEFAULT 0
);

CREATE TABLE lgu_summary (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    province_name VARCHAR(255) NOT NULL,
    total_districts INT NOT NULL DEFAULT 0
);

