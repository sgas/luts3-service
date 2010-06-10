function(doc) {

    var year      = null;
    var month     = null;
    var day       = null;
    var host      = null;
    var user      = null;
    var vo_issuer = null;
    var vo        = null;
    var vo_group  = null;
    var vo_role   = null;

    var cpu_time  = 0;
    var wall_time = 0;

    basedate = doc.create_time;
    if (doc.end_time && doc.end_time.substring(0,4) != "1970") {
        basedate = doc.end_time;
    }
    year  = parseInt(basedate.substring(0,4), 10);
    month = parseInt(basedate.substring(5,7), 10);
    day   = parseInt(basedate.substring(8,10), 10);

    if (doc.machine_name)      { host = doc.machine_name; }

    if (doc.global_user_name)  { user = doc.global_user_name; }
    else if (doc.machine_name && doc.local_user_id) {
        user = doc.machine_name + ":" + doc.local_user_id;
    }

    if (doc.vo_issuer)        { vo_issuer = doc.vo_issuer; }

    if (doc.vo_name && doc.vo_name.substring(0) != "/") {
        vo = doc.vo_name;
    }
    if (doc.vo_attrs) {
        voi = doc.vo_attrs[0];
        if (voi.group) { vo_group = voi.group; }
        if (voi.role)  { vo_role  = voi.role;  }
    }

    if (doc.cpu_duration)   { cpu_time  = doc.cpu_duration; }
    if (doc.wall_duration)  { wall_time  = doc.wall_duration; }

    key = [year,month,day,host,user,vo_issuer,vo,vo_group,vo_role];
    value = [cpu_time, wall_time];

    emit(key, value);
}

--

function(keys, values, rereduce) {

    count      = 0;
    cpu_total  = 0;
    wall_total = 0;

    if (!rereduce) {
        count = values.length;
        for (i in values) {
            cpu_total     += values[i][0];
            wall_total    += values[i][1];
        }
    }
    else {
        for (i in values) {
            count         += values[i][0];
            cpu_total     += values[i][1];
            wall_total    += values[i][2];
        }
    }

    return [count, cpu_total, wall_total ];
}

