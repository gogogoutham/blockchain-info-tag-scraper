CREATE TABLE blockchain_info_tag (
    address VARCHAR(50),
    tag VARCHAR(50),
    link TEXT,
    verified BOOLEAN,
    time TIMESTAMP WITH TIME ZONE, 
    PRIMARY KEY(address));

CREATE TABLE blockchain_info_tag_all (
    address CHAR(64),
    tag VARCHAR(50),
    link TEXT,
    verified BOOLEAN,
    time TIMESTAMP WITH TIME ZONE, 
    PRIMARY KEY(address, tag, link, verified));