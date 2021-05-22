--4.1. База данных содержит список аэропортов практически всех крупных городов России. В большинстве городов есть только один аэропорт. Исключение составляет:

SELECT city,
       count(*)
FROM DST_PROJECT.airports
GROUP BY city
ORDER BY 2 DESC



--4.2. Таблица рейсов содержит всю информацию о прошлых, текущих и запланированных рейсах. Сколько всего статусов для рейсов определено в таблице?
SELECT COUNT (*)
FROM
  (SELECT DISTINCT f.status
   FROM DST_PROJECT.flights AS f) AS statuses
   
--Какое количество самолетов находятся в воздухе на момент среза в базе (статус рейса «самолёт уже вылетел и находится в воздухе»
SELECT count(*)
FROM DST_PROJECT.flights AS f
WHERE f.status = 'Departed'

--Места определяют схему салона каждой модели. Сколько мест имеет самолет модели  (Boeing 777-300)?
select count(*)
from DST_PROJECT.seats as s  
where s.aircraft_code='773'

--Сколько состоявшихся (фактических) рейсов было совершено между 1 апреля 2017 года и 1 сентября 2017 года?
SELECT count(*)
FROM DST_PROJECT.flights AS f
WHERE status='Arrived'
  AND date_part('year', f.actual_arrival)=2017
  AND (date_part('month', f.actual_arrival) BETWEEN 4 AND 8)
  
  
-- Сколько самолетов моделей типа Boeing, Sukhoi Superjet, Airbus находится в базе авиаперевозок?
SELECT count(*),
       'Boeing'
FROM DST_PROJECT.aircrafts AS a
WHERE a.model like 'Boeing%'
UNION
SELECT count(*),
       'Superjet'
FROM DST_PROJECT.aircrafts AS a
WHERE a.model like '%Superjet%'
UNION
SELECT count(*),
       'Airbus'
FROM DST_PROJECT.aircrafts AS a
WHERE a.model like 'Airbus%'

--В какой части (частях) света находится больше аэропортов?
WITH xx AS
  (WITH x AS
     (SELECT timezone,
             count(*) AS num
      FROM DST_PROJECT.airports AS a
      GROUP BY timezone
      ORDER BY 1) SELECT timezone,
                         num,
                         'Asia' AS ZONE
   FROM x
   WHERE timezone like 'Asia%'
   UNION SELECT timezone,
                num,
                'Europe' AS ZONE
   FROM x
   WHERE timezone like 'Europe%'
   UNION SELECT timezone,
                num,
                'Australia' AS ZONE
   FROM x
   WHERE timezone like 'Australia%' )
SELECT ZONE,
       count(*)
FROM xx
GROUP BY ZONE

--У какого рейса была самая большая задержка прибытия за все время сбора данных? Введите id рейса (flight_id).
SELECT flight_id,
       extract(epoch
               FROM actual_arrival - scheduled_arrival) AS delay
FROM DST_PROJECT.flights
WHERE actual_arrival IS NOT NULL
  AND actual_arrival>scheduled_arrival
ORDER BY delay DESC

--Когда был запланирован самый первый вылет, сохраненный в базе данных?
SELECT min(scheduled_departure)
FROM DST_PROJECT.flights

--Сколько минут составляет запланированное время полета в самом длительном рейсе?
select max(extract(epoch from scheduled_arrival - scheduled_departure) / 60)
from DST_PROJECT.flights 


--Между какими аэропортами пролегает самый длительный по времени запланированный рейс?
select distinct extract(epoch from scheduled_arrival - scheduled_departure) as dur, departure_airport, arrival_airport
from DST_PROJECT.flights
order by 1 desc

--Сколько составляет средняя дальность полета среди всех самолетов в минутах? Секунды округляются в меньшую сторону (отбрасываются до минут).
select avg(extract(epoch from scheduled_arrival - scheduled_departure) / 60) 
from DST_PROJECT.flights

--Мест какого класса у SU9 больше всего?
SELECT fare_conditions,
       count(*)
FROM DST_PROJECT.aircrafts a
JOIN DST_PROJECT.seats s ON a.aircraft_code = s.aircraft_code
WHERE a.aircraft_code = 'SU9'
GROUP BY fare_conditions

--Какую самую минимальную стоимость составило бронирование за всю историю?
select min(total_amount)
from DST_PROJECT.bookings b 

--Какой номер места был у пассажира с id = 4313 788533
select seat_no
from DST_PROJECT.tickets t join DST_PROJECT.boarding_passes b on t.ticket_no = b.ticket_no  
where passenger_id = '4313 788533'

--Анапа — курортный город на юге России. Сколько рейсов прибыло в Анапу за 2017 год?
SELECT count(*)
FROM DST_PROJECT.flights f
JOIN DST_PROJECT.airports a ON f.arrival_airport = a.airport_code
WHERE a.city = 'Anapa'
  AND date_part('year', f.actual_arrival)=2017
  
--Сколько рейсов из Анапы вылетело зимой 2017 года?
SELECT count(*)
FROM DST_PROJECT.flights f
JOIN DST_PROJECT.airports a ON f.departure_airport = a.airport_code
WHERE a.city = 'Anapa'
  AND date_part('year', f.actual_departure)=2017
  AND (date_part('month', f.actual_departure) in (12,
                                                  1,
                                                  2))  
												  
												  
--Посчитайте количество отмененных рейсов из Анапы за все время.
SELECT count(*)
FROM DST_PROJECT.flights f
JOIN DST_PROJECT.airports a ON f.departure_airport = a.airport_code
WHERE a.city = 'Anapa'
  AND status='Cancelled'												  
											

--Сколько рейсов из Анапы не летают в Москву?
SELECT count(flight_no)
FROM DST_PROJECT.flights f
JOIN DST_PROJECT.airports a ON f.departure_airport = a.airport_code
WHERE a.city = 'Anapa'
  AND f.arrival_airport not in
    (SELECT airport_code
     FROM DST_PROJECT.airports
     WHERE city = 'Moscow' )											
	 
	 
--Какая модель самолета летящего на рейсах из Анапы имеет больше всего мест?
WITH seats_view AS
  (SELECT aircraft_code,
          count(*) AS num_seats
   FROM DST_PROJECT.seats
   GROUP BY aircraft_code)
SELECT DISTINCT v.model,
                num_seats
FROM DST_PROJECT.flights f
JOIN DST_PROJECT.airports a ON f.departure_airport = a.airport_code
JOIN seats_view s ON f.aircraft_code = s.aircraft_code
JOIN DST_PROJECT.aircrafts v ON f.aircraft_code = v.aircraft_code
WHERE a.city = 'Anapa'
ORDER BY num_seats DESC	 



--зимние рейсы из Анапы + рейсы обратно (надо вернуть самолет обратно, чтобы на нем совершать рейсы вновь)
with anapa_from as 
(SELECT flight_no, scheduled_departure, scheduled_arrival, actual_departure, actual_arrival, departure_airport, arrival_airport, aircraft_code, fare_conditions, amount, 'leave' as direction
FROM dst_project.flights f left join dst_project.boarding_passes b on f.flight_id = b.flight_id left join dst_project.ticket_flights t on b.ticket_no = t.ticket_no AND b.flight_id = t.flight_id
WHERE departure_airport = 'AAQ'
  AND (date_trunc('month', scheduled_departure) in ('2017-01-01','2017-02-01', '2017-12-01'))
  AND status not in ('Cancelled')
)
SELECT * from anapa_from

union all
--обратные рейсы
SELECT flight_no, scheduled_departure, scheduled_arrival, actual_departure, actual_arrival, departure_airport, arrival_airport, aircraft_code, fare_conditions, amount, 'return' as direction
FROM dst_project.flights f left join dst_project.boarding_passes b on f.flight_id = b.flight_id left join dst_project.ticket_flights t on b.ticket_no = t.ticket_no AND b.flight_id = t.flight_id
WHERE arrival_airport = 'AAQ'
  AND (date_trunc('month', scheduled_departure) in ('2017-01-01','2017-02-01', '2017-12-01'))
  AND status not in ('Cancelled')
