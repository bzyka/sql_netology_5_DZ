# sql_netology_5_DZ
--=============== МОДУЛЬ 5. РАБОТА С POSTGRESQL =======================================
--= ПОМНИТЕ, ЧТО НЕОБХОДИМО УСТАНОВИТЬ ВЕРНОЕ СОЕДИНЕНИЕ И ВЫБРАТЬ СХЕМУ PUBLIC===========
SET search_path TO public;

--======== ОСНОВНАЯ ЧАСТЬ ==============

--ЗАДАНИЕ №1
--Сделайте запрос к таблице payment и с помощью оконных функций добавьте вычисляемые колонки согласно условиям:
--Пронумеруйте все платежи от 1 до N по дате платежа

SELECT  customer_id, payment_id, payment_date,
       ROW_NUMBER() OVER (ORDER BY payment_date) AS row_number
FROM payment

--Пронумеруйте платежи для каждого покупателя, сортировка платежей должна быть по дате платежа

SELECT  customer_id, payment_id, payment_date,
       ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY payment_date) AS row_number_per_customer
FROM payment

--Посчитайте нарастающим итогом сумму всех платежей для каждого покупателя, сортировка должна 
--быть сперва по дате платежа, а затем по размеру платежа от наименьшей к большей



SELECT customer_id, payment_id, payment_date, amount,
       SUM(amount) OVER (
           PARTITION BY customer_id
           ORDER BY payment_date ASC, amount ASC
       ) AS cumulative_sum
FROM payment
ORDER BY customer_id, payment_date, amount



--Пронумеруйте платежи для каждого покупателя по размеру платежа от наибольшего к
--меньшему так, чтобы платежи с одинаковым значением имели одинаковое значение номера.

SELECT  customer_id, payment_id, payment_date, amount,
       DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS dense_rank_per_customer
FROM payment

--Можно составить на каждый пункт отдельный SQL-запрос, а можно объединить все колонки в одном запросе.

SELECT customer_id, payment_id, payment_date, 
	row_number() over (order by payment_date) as "Номер платежа по дате",
	row_number() over (partition by customer_id order by payment_date) as "Номер платежа покупателя по дате",
	sum(p.amount) over (partition by p.customer_id order by payment_date ASC, amount ASC) AS cumulative_sum,
	dense_rank() over (partition by p.customer_id order by amount desc)
FROM payment p
order by customer_id, dense_rank


--ЗАДАНИЕ №2
--С помощью оконной функции выведите для каждого покупателя стоимость платежа и стоимость 
--платежа из предыдущей строки со значением по умолчанию 0.0 с сортировкой по дате платежа.

 SELECT
    customer_id, payment_id, payment_date,
    payment_date as дата_платежа,
    amount AS текущий_платеж,
    LAG(amount, 1, 0.0) OVER(PARTITION BY customer_id ORDER BY payment_date) AS предыдущий_платеж
FROM
    payment
ORDER BY
    customer_id,
    payment_date


--ЗАДАНИЕ №3
--С помощью оконной функции определите, на сколько каждый следующий платеж покупателя больше или меньше текущего.


SELECT
    customer_id, payment_id, payment_date, amount AS current_payment,
    amount - LEAD(amount, 1) OVER(PARTITION BY customer_id ORDER BY payment_date) AS difference
FROM
    payment

--ЗАДАНИЕ №4
--С помощью оконной функции для каждого покупателя выведите данные о его последней оплате аренды.

WITH ranked_payments AS (
    SELECT
        customer_id,
        payment_date,
        payment_id,
        amount,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY payment_date DESC) as rn
    FROM payment
)
SELECT
    customer_id,
    payment_id,
    payment_date,
    amount
FROM ranked_payments
WHERE rn = 1

--======== ДОПОЛНИТЕЛЬНАЯ ЧАСТЬ ==============

--ЗАДАНИЕ №1
--С помощью оконной функции выведите для каждого сотрудника сумму продаж за август 2005 года 
--с нарастающим итогом по каждому сотруднику и по каждой дате продажи (без учёта времени) 
--с сортировкой по дате.

SELECT
    staff.staff_id,
    DATE_TRUNC('day', payment.payment_date)::DATE AS date_only,
    SUM(payment.amount) AS daily_sales_sum,
    SUM(SUM(payment.amount)) OVER (
        PARTITION BY staff.staff_id
        ORDER BY DATE_TRUNC('day', payment.payment_date)
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_sales_sum
FROM
    payment
JOIN
    staff ON payment.staff_id = staff.staff_id
WHERE
    EXTRACT(YEAR FROM payment.payment_date) = 2005
    AND EXTRACT(MONTH FROM payment.payment_date) = 8
GROUP BY
    staff.staff_id,
    DATE_TRUNC('day', payment.payment_date)
ORDER BY
    staff.staff_id,
    DATE_TRUNC('day', payment.payment_date);




--ЗАДАНИЕ №2
--20 августа 2005 года в магазинах проходила акция: покупатель каждого сотого платежа получал
--дополнительную скидку на следующую аренду. С помощью оконной функции выведите всех покупателей,
--которые в день проведения акции получили скидку

WITH numbered_payments AS (
    SELECT
        payment_id,
        customer_id,
        payment_date,
        ROW_NUMBER() OVER (ORDER BY payment_date) AS row_num
    FROM
        payment
    WHERE
        payment_date::DATE = '2005-08-20'
)
SELECT
    customer_id, payment_date, row_num
FROM
    numbered_payments
WHERE
    row_num % 100 = 0



--ЗАДАНИЕ №3
--Для каждой страны определите и выведите одним SQL-запросом покупателей, которые попадают под условия:
-- 1. покупатель, арендовавший наибольшее количество фильмов
-- 2. покупатель, арендовавший фильмов на самую большую сумму
-- 3. покупатель, который последним арендовал фильм

WITH rental_count AS (
    SELECT c.customer_id,
           c.first_name || ' ' || c.last_name AS customer_fullname,
           COUNT(r.rental_id) AS total_rentals,
           SUM(p.amount) AS total_amount,
           MAX(r.rental_date) AS last_rental_date,
           cu.country
    FROM customer c
    INNER JOIN address a ON c.address_id = a.address_id
    INNER JOIN city ci ON a.city_id = ci.city_id
    INNER JOIN country cu ON ci.country_id = cu.country_id
    LEFT JOIN rental r ON c.customer_id = r.customer_id
    LEFT JOIN payment p ON r.rental_id = p.rental_id
    WHERE r.return_date IS NOT NULL
    GROUP BY c.customer_id, c.first_name, c.last_name, cu.country
),
ranked_customers AS (
    SELECT *,
           RANK() OVER(PARTITION BY country ORDER BY total_rentals DESC) AS rent_rank,
           RANK() OVER(PARTITION BY country ORDER BY total_amount DESC) AS amount_rank,
           RANK() OVER(PARTITION BY country ORDER BY last_rental_date DESC) AS date_rank
    FROM rental_count
),
final_result AS (
    SELECT country as Страна,
           MAX(CASE WHEN rent_rank = 1 THEN customer_fullname END) AS "Арендовавший наибольшее кол-во",
           MAX(CASE WHEN amount_rank = 1 THEN customer_fullname END) AS "На самую большую сумму",
           MAX(CASE WHEN date_rank = 1 THEN customer_fullname END) AS "Последним арендовал фильм"
    FROM ranked_customers
    GROUP BY country
)
SELECT *
FROM final_result
ORDER BY Страна ASC



