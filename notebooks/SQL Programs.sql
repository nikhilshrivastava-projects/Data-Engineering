select name, department, salary
RANK() OVER (PARTITION BY department ORDER BY Salary DESC) as dept_rank
From employees

select name, department, salary