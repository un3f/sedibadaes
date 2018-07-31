-- creamos un Esquema donde guardar todo
drop schema if exists eaht216 cascade;

create schema eaht216;

set search_path to eaht216;

-- tabla plana con una única columna para mirar los primeros registros
create table plana(
  renglon serial,
  unica_columna text
);

-- importamos la tabla sin separador de campos ni de texto
copy plana (unica_columna) 
  from 'c:\temp\eph\usu_individual_T216.txt'
  (format csv, delimiter E'\1', quote E'\2');

-- miramos los primeros renglones
select unica_columna
  from plana
  order by renglon
  limit 10;

-- metadato de las columnas de la tabla final
create table columnas(
  tabla text,
  columna text,
  orden integer, 
  tipo text,
  primary key (tabla, columna)
);

-- encontramos el delimitador de campos (;) vamos a cargar el metadato de las columnas desde la primera fila de la tabla plana
insert into columnas select 'individual', columna, row_number() over() from regexp_split_to_table((select unica_columna from plana where renglon=1),';') columna;

select * 
  from columnas
  where tabla='individual'
  order by orden;

-- creamos la tabla vacia
create table usu_individual ();

-- agrego las columnas de a una en formato texto
do
$do$
declare
  v_columna text;
begin
  for v_columna in  
    select columna
      from columnas
      where tabla='individual'
      order by orden
  loop
    execute 'alter table usu_individual add column '||v_columna||' text';
  end loop;
end;
$do$;

-- miramos la tabla vacía
select * from usu_individual;

-- importamos la tabla con los delimitadores de campo y de texto
copy usu_individual 
  from 'c:\temp\eph\usu_individual_T216.txt'
  (format csv, delimiter ';', quote '"', header true);

-- miramos la tabla con datos (todos textos)
select * from usu_individual
  limit 100;

-- contamos los registros importados
select count(*) from usu_individual;

-- en la columna ipcf vemos que el decimal es la ','
-- en la columna ch05 vemos que la fecha es dd/mm/yyyy

-- revisemos qué campos con compatibles con:
-- solo números sin decimales (bigint)
-- números con decimales (con , o .)
-- fechas (dd/mm/yyyy, mm/dd/yyyy, yyyy/mm/dd, yyyy-mm-dd)
-- todo lo demás es texto

-- ¿qué hacemos con los NULL, con los texto que son espacios en blanco sin longitud o en blanco, etc...?

-- con expresiones regulares podemos ver si todos los campos son dígitos como el caso de ano4
--      ~      significa contiene la "expresión regular" (o sea que del lado derecho se puede poner un texto o códigos que representan textos)
--      \d     es ún dígito o sea un caracter del 0 al 9
--      \d*    es cero o más dígitos (el * indica repetición)
--      ^\d*$  singifca que todo el campo contiene cero o más dígitos el ^ signifca que controla desde el principo
--   bool_and  es una función de agregación (como sum, avg, min, max, count) que revisa todos los renglones de una columna, en este caso indica si todo cumple: and

select bool_and(ano4 ~ '^\d*$') 
  from usu_individual;

-- para controlar fechas se puede usar el concepto de rangos:
--       [0-3]   es un dígito del 0 al 3 (para el número del comienzo de mes)

select bool_and(ano4 ~ '^\d*$') as "ano4 es número", 
       bool_and(codusu ~ '^\d*$') as "codusu es número", 
       bool_and(ch05 ~ '^\d*$') as "ch05 es número",
       bool_and(ch05 ~ '^[0-3]\d/[0-1]\d/\d\d\d\d$') as "ch05 es fecha",
       bool_and(codusu ~ '^[0-3]\d/[0-1]\d/\d\d\d\d$') as "codusu es fecha"
  from usu_individual;


-- revisemos todas las columnas para ver cuáles se pueden transformar a entero:
do
$do$
declare
  v_columna text;
  v_respuesta boolean;
begin
  for v_columna in  
    select columna
      from columnas
      where tabla='individual' and tipo is null
      order by orden
  loop
    execute $$select bool_and($$||v_columna||$$ ~ '^\d*$') from usu_individual$$ into v_respuesta;
    if v_respuesta then
      update columnas set tipo='bigint' where columna=v_columna and tabla='individual';
      raise notice 'cambio % a bigint', v_columna;
      -- execute 'alter table usu_individual alter column '||v_columna||' type bigint using '||v_columna||'::bigint';
    end if;
  end loop;
end;
$do$;


-- revisemos todas las columnas para ver cuáles se pueden transformar a decimal:
-- son decimales los números que tienen una coma
--    ^\d*,\d+$   (0 o más dígitos una coma y uno o más dígitos)
--   pero también los que no la tienen, algo más genérico sería:
--    ^\d*,?\d+$  (0 o más dígitos quizás seguidos por una coma, luego uno o más dígitos), acá  el ? significa opcional
do
$do$
declare
  v_columna text;
  v_respuesta boolean;
begin
  for v_columna in  
    select columna
      from columnas
      where tabla='individual' and tipo is null
      order by orden
  loop
    execute $$select bool_and($$||v_columna||$$ ~ '^\d*,\d+$') from usu_individual$$ into v_respuesta;
    if v_respuesta then
      update columnas set tipo='decimal' where columna=v_columna and tabla='individual';
      raise notice 'cambio % a decimal', v_columna;
      execute $$alter table usu_individual alter column $$||v_columna||$$ type decimal using replace($$||v_columna||$$,',','.')::decimal$$;
    end if;
  end loop;
end;
$do$;

-- revisemos todas las fechas dd/mm/yy
do
$do$
declare
  v_columna text;
  v_respuesta boolean;
begin
  for v_columna in  
    select columna
      from columnas
      where tabla='individual' and tipo is null
      order by orden
  loop
    execute $$select bool_and($$||v_columna||$$ ~ '^[0-3]\d/[0-1]\d/\d\d\d\d$') from usu_individual$$ into v_respuesta;
    if v_respuesta then
      update columnas set tipo='date' where columna=v_columna and tabla='individual';
      raise notice 'cambio % a date', v_columna;
      execute $$alter table usu_individual alter column $$||v_columna||$$ type date using to_date($$||v_columna||$$,'dd/mm/yyyy')$$;
    end if;
  end loop;
end;
$do$;

select *
  from columnas
  order by columna;

select * 
  from usu_individual
  limit 100; 
