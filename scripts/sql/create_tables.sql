CREATE TABLE IF NOT EXISTS monzo.monzo_transactions (
id varchar(250) PRIMARY KEY,
date date,
description varchar(250),
amount float,
category varchar(250),
decline_reason varchar(250),
decline INTEGER,
merchant_id varchar(250),
merchant_description varchar(250),
emoji varchar(250),
merchant_category varchar(250),
suggested_tags varchar(250),
notes varchar(250)
);