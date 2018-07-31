select regexp_matches(linea::text, '((?:[^;"]+)|(?:"(?:[^"]*(?:"")*)*"))(;|$)','g')
  from (select 'uno;dos;tre;"cuatro";"cinco""5""";"punto;y;coma"' as linea) linea
