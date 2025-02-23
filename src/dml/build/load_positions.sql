copy (
    select position_id
    , abbreviation
    , position_name
    from
    (
        values
            (1, 'P', 'pitcher'),
            (2, 'C', 'catcher'),
            (3, '1B', 'first_base'),
            (4, '2B', 'second_base'),
            (5, '3B', 'third_base'),
            (6, 'SS', 'shortstop'),
            (7, 'LF', 'left_field'),
            (8, 'CF', 'center_field'),
            (9, 'RF', 'right_field'),
            (100, 'DH', 'designated_hitter'),
            (200, 'IF', 'infield'),
            (300, 'OF', 'outfield'),
            (400, 'PH', 'pinch_hitter'),
            (500, 'PR', 'pinch_runner'),
            (600, 'X', 'other')
    ) positions(position_id, abbreviation, position_name)
)  to 'data/build/positions.csv'
(
    overwrite true
)
;