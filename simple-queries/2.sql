select *
from cast_info
join role_type on cast_info.role_id = role_type.id
join person_info on person_info.id = cast_info.id;
