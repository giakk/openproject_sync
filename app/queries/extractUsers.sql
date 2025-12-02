SELECT 
    m.Man_id AS GimiID,
    m.Man_nominativo AS Nominativo,
    m.Man_cellulare AS Cell,
    e.Email_indirizzo AS Email
FROM 
    Tab_Manutentori m
LEFT JOIN 
    Indirizzi_Email e ON e.Email_chiave_id = m.Man_id 
                     AND e.Email_tabella = 6
WHERE 
    m.Man_data_fine_att IS NULL