alter table bgjob_job
    add column request_id text;

alter table bgjob_dead_job
    add column request_id text;

update bgjob_job
    set request_id = ''
    where request_id is null;

update bgjob_dead_job
    set request_id = ''
    where request_id is null;

alter table bgjob_job
    alter column request_id set default '',
    alter column request_id set not null;

alter table bgjob_dead_job
    alter column request_id set default '',
    alter column request_id set not null;
