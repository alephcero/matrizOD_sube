select *
from tramos t 
limit 5;


select count(*)
from viajes t 
limit 5;

select *
from tramos
where id_tarjeta in (
	with tabla_destinos_nulos as (
		select id_tarjeta,count(id_tarjeta) = sum(CASE when d_h3 = ''  THEN 1 else 0 end) as todos_destinos_null
		from tramos d
		group by id_tarjeta
	)
	select id_tarjeta
	from tabla_destinos_nulos
	where todos_destinos_null = true
	limit %i
)
order by id_tarjeta,id_viaje,id_tramo;

--1457075
select sum(CASE when d_h3 is NULL  THEN 1 else 0 end)
from tramos t;

--9285695
select count(*)
from tramos t;

ALTER TABLE tramos ADD PRIMARY KEY (id_tarjeta, id_viaje,id_tramo);



select ST_SetSRID(ST_MakePoint(o_lat,o_lon),4326) as o_geom
from tramos t 
limit 5;

ALTER TABLE tramos ADD COLUMN o_geom geometry(Point,4326);
ALTER TABLE tramos ADD COLUMN d_geom geometry(Point,4326);

UPDATE tramos
SET o_geom = ST_SetSRID(ST_MakePoint(o_lat,o_lon),4326);

UPDATE tramos
SET d_geom = ST_SetSRID(ST_MakePoint(d_lat,d_lon),4326);



select *
from pares_latlon
;


--67524


select * from fracciones f 
limit 5;

CREATE TABLE pares_latlon 
as (
	select distinct lat,lon
	from (
	select distinct o_lat as lat, o_lon as lon
	from tramos
	union 
	select distinct d_lat as lat, d_lon as lon
	from tramos
	) as tabla
);


ALTER TABLE pares_latlon ADD COLUMN geom geometry(Point,4326);

UPDATE pares_latlon
SET geom = ST_SetSRID(ST_MakePoint(lon,lat),4326);

--spatial join fracciones


ALTER TABLE fracciones ADD COLUMN geom geometry;

UPDATE fracciones
SET geom =  st_transform(wkb_geometry, 4326); 

-- crear id fraccion en pares
ALTER TABLE pares_latlon ADD COLUMN id_fraccion varchar;

-- popular id fraccion con el id del join espacial
with join_espacial as (
	select f.link ,p.lat,p.lon
	from pares_latlon as p
	join fracciones as f
	ON ST_Contains(f.geom, p.geom)
)
UPDATE pares_latlon
SET id_fraccion = link
FROM join_espacial
WHERE pares_latlon.lat = join_espacial.lat
and pares_latlon.lon = join_espacial.lon;



--partido
ALTER TABLE pares_latlon ADD COLUMN id_partido varchar;

select *
from pares_latlon pl
limit 5;


UPDATE pares_latlon
SET id_partido = SUBSTRING(id_fraccion ,0 , 5 ); 


