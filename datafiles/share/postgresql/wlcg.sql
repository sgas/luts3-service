create schema wlcg;

create table wlcg.countries (
    country_id   serial                     not null primary key,
    country_name character varying(255)     not null unique
);

-- insert into wlcg.countries (country_name) select name from pledges.countries;


create table wlcg.sites (
    site_id     serial                  not null primary key,
    country_id  integer                 not null references wlcg.countries (country_id),
    site_name   character varying(255)  not null unique
);

/*
insert into wlcg.sites (country_id, site_name)
    select
        wlcg.countries.country_id,
        pledges.sites.name
    from    pledges.sites
       join pledges.countries on pledges.sites.country_id = pledges.countries.id
       join wlcg.countries on wlcg.countries.country_name = pledges.countries.name
;
*/

create table wlcg.tiers (
    machine_name_id  integer                 not null references machinename (id),
    vo_name          character varying(255)  not null,
    tier_name        character varying(255)  not null,
    primary key (machine_name_id, vo_name)
);

-- insert into wlcg.tiers (machine_name_id, vo_name, tier_name) select machine_name_id, vo_name, tier_name from pledges.tiers;


create table wlcg.pledges (
    pledge_id   serial                  not null primary key,
    site_id     integer                 references wlcg.sites (site_id),
    country_id  integer                 not null references wlcg.countries (country_id),
    vo_name     character varying(255)  not null,
    tier_name   character varying(255)  not null,
    pledge      double precision        not null,
    validity    tsrange                 not null
);

/*
insert into wlcg.pledges (site_id, country_id, vo_name, tier_name, pledge, validity) 
    select
        wlcg.sites.site_id,
        wlcg.countries.country_id,
        pledges.pledges.vo_name,
        pledges.pledges.tier,
        pledges.pledges.pledge,
        pledges.pledges.validity
    from     pledges.pledges
        left join pledges.countries on pledges.pledges.country_id = pledges.countries.id
        left join wlcg.countries on wlcg.countries.country_name = pledges.countries.name
        left join pledges.sites on pledges.sites.id = pledges.pledges.site_id
        left join wlcg.sites on wlcg.sites.site_name = pledges.sites.name
    ;
*/


create table wlcg.machinename_site_junction (
    machine_name_id integer   not null unique references machinename (id),
    site_id         integer   not null references wlcg.sites (site_id)
);

/*
insert into wlcg.machinename_site_junction (machine_name_id, site_id)
    select
        pledges.machinename_site_junction.machinename_id,
        wlcg.sites.site_id
    from     pledges.machinename_site_junction
        join pledges.sites on pledges.sites.id = pledges.machinename_site_junction.site_id
        join wlcg.sites on wlcg.sites.site_name =  pledges.sites.name
    ;
*/
create view wlcg.usagedata as
    select
        global_user_name_id,
        vo_information_id,
        usagedata.machine_name_id,
        status_id,
        coalesce(node_count,1)      as node_count,
        coalesce(processors,1)      as processors,
        start_time,
        end_time,
        coalesce(cpu_duration,0)                          as cpu_duration, -- seconds
        coalesce(wall_duration,0)                         as wall_duration, -- seconds 
        coalesce(hostscalefactors_data.scale_factor,1.0)  as hs06
    from          usagedata
        left join hostscalefactors_data on usagedata.machine_name_id = hostscalefactors_data.machine_name_id and
                                           hostscalefactors_data.scalefactor_type_id = ((
                                            select
                                                hostscalefactor_types.id
                                            from hostscalefactor_types
                                            where hostscalefactor_types.factor_type = 'hepspec06'
                                            limit 1
                                        )) and usagedata.start_time <@ hostscalefactors_data.validity_period
    ;

        

create view wlcg.urs as
     select
        voinformation.vo_name             as vo_name,
        voinformation.vo_type             as vo_type,
        voinformation.vo_attributes[1][1] as vo_group,
        voinformation.vo_attributes[1][2] as vo_role,
        usagedata.machine_name_id         as machine_name_id,
        msj.site_id                       as site_id,
        countries.country_id              as country_id,
        machinename.machine_name          as machine_name,
        site.site_name                    as site_name,
        countries.country_name            as country_name,
        tiers.tier_name                   as tier_name,
        coalesce(usagedata.processors,1)  as ncores,
        coalesce(usagedata.node_count,1)  as node_count,
        coalesce(usagedata.wall_duration, 0)/3600.0 as wallh,
        coalesce(usagedata.cpu_duration, 0)/3600.0 as cpuh,
        coalesce(usagedata.wall_duration, 0)/3600.0 * coalesce(hs.scale_factor,1.0) as hs06wallh,
        coalesce(usagedata.cpu_duration, 0)/3600.0 * coalesce(hs.scale_factor,1.0) as hs06cpuh,
        usagedata.end_time        as end_time,
        jobstatus.status          as status
    from          usagedata 
        left join hostscalefactors_data hs on usagedata.machine_name_id = hs.machine_name_id
                                          and hs.scalefactor_type_id = ( select id from hostscalefactor_types where factor_type = 'hepspec06' LIMIT 1 )
                                          and usagedata.start_time <@ hs.validity_period
             join machinename on usagedata.machine_name_id = machinename.id
        left join voinformation on usagedata.vo_information_id = voinformation.id
        left join wlcg.machinename_site_junction msj on msj.machine_name_id = usagedata.machine_name_id
        left join wlcg.sites site using(site_id)
        left join wlcg.tiers tiers on tiers.machine_name_id = usagedata.machine_name_id and (tiers.vo_name = voinformation.vo_name or tiers.vo_name = '*')
        left join wlcg.countries countries using(country_id)
        left join jobstatus ON usagedata.status_id = jobstatus.id
;



/*
select 
    pledge.vo_name as vo_name, 
    country.country_name as Country, 
    coalesce(site.site_name, (select site_name from wlcg.sites where site_id = urs.site_id)) as Site, 
    sum(hs06cpuh) / sum(hs06wallh * ncores) as Efficiency, 
    sum(hs06wallh * ncores) as kHS06h, 
    sum(hs06wallh * ncores)/1000 / (extract(epoch from '72 hours'::interval)/3600.0) / pledge.pledge * 100 as Percent_of_pledge 
from     wlcg.pledges pledge 
    join wlcg.countries country using(country_id) 
    left join wlcg.sites site using(site_id) 
    left join wlcg.urs urs on (urs.site_id = site.site_id or site.site_id is null) 
            and urs.country_id = country.country_id 
            and urs.vo_name = pledge.vo_name 
            and urs.end_time between (now()::timestamp - '72 hours'::interval - '1 hours'::interval) and (now()::timestamp - '1 hours'::interval) 
where pledge.tier_name = 'ndgf-t1' 
    and now()::timestamp <@ pledge.validity 
group by 
    pledge.vo_name, 
    Site, 
    pledge.pledge, 
    Country 
order by 
    pledge.vo_name, 
    Country, 
    Site 
;

 */ 
