-- Cost centres as validity intervals. A cost centre that moves department is what makes
-- the point-in-time join necessary: a March transaction has to be reported against the
-- department it belonged to in March, not the one it belongs to now.
{{ scd2('dim_cost_center_src', ['cc_code'], ['name', 'dept_code']) }}
