create table if not exists customer
(
    id        serial primary key,
    processed boolean default false,
    name      varchar(255) not null unique
);