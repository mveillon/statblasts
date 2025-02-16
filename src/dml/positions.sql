copy (
    select position_id
    , position_name
    from
    (
        values
            (1, 'pitcher'),
            (2, 'catcher'),
            (3, 'first_base'),
            (4, 'second_base'),
            (5, 'third_base'),
            (6, 'shortstop'),
            (7, 'left_field'),
            (8, 'center_field'),
            (9, 'right_field')
    ) positions(position_id, position_name)
)  to 'data/build/positions.csv'
(
    overwrite true
)
;