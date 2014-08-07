CREATE TABLE tag (
    address VARCHAR(50),
    tag VARCHAR(50),
    link TEXT,
    verified BOOLEAN,
    time TIMESTAMP WITH TIME ZONE, 
    PRIMARY KEY(address));

CREATE TABLE tag_all (
    address CHAR(64),
    tag VARCHAR(50),
    link TEXT,
    verified BOOLEAN,
    time TIMESTAMP WITH TIME ZONE, 
    PRIMARY KEY(address, tag, link, verified));
