copy (
    select position_id
    , abbreviation
    , position_name
    from
    (
        values
            (1, 'P', 'pitcher'),
            (2, 'C', 'catcher'),
            (3, '1B', 'first base'),
            (4, '2B', 'second base'),
            (5, '3B', 'third base'),
            (6, 'SS', 'shortstop'),
            (7, 'LF', 'left field'),
            (8, 'CF', 'center field'),
            (9, 'RF', 'right field'),
            (100, 'DH', 'designated hitter'),
            (200, 'IF', 'infield'),
            (300, 'OF', 'outfield'),
            (400, 'PH', 'pinch hitter'),
            (500, 'PR', 'pinch runner'),
            (600, 'X', 'other')
    ) positions(position_id, abbreviation, position_name)
)  to 'data/build/positions.csv'
(
    overwrite true
)
;