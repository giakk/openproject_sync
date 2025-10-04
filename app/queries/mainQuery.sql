Select iif(Pre_sigla_commessa<>'', concat(Pre_sigla_commessa,'/', Pre_numero_commessa), convert(nvarchar, Pre_numero_commessa)) as NrCommessa,	
	format(Pre_data_commessa, 'dd/MM/yyyy') as AperturaCommessa, 
	coalesce(PreI_impianto,'') as CodImpianto, 
	coalesce(Imp_nominativo,'') as Imp_nominativo,	concat(Imp_pref_via,' ',Imp_via) As Imp_indirizzo, coalesce(Imp_locazione,'') as Imp_locazione,
	coalesce(Imp_cap,'') as Imp_cap ,coalesce(Imp_localita,'') as Imp_localita, coalesce(Imp_prov,'') as Imp_prov,
	rtrim(coalesce(Amm_anagr,' ', amm_est_anagr)) as Amm_nominativo, coalesce(Amm_tel_ufficio,'') as Amm_tel_ufficio, coalesce(Amm_cellulare,'') as Amm_cellulare,

	(SELECT coalesce((SELECT DISTINCT SUBSTRING((SELECT '; ' + Email_indirizzo AS [text()] 
		FROM Indirizzi_Email st1Indirizzi_Email 
		WHERE st1Indirizzi_Email.Email_tabella=3 and st1Indirizzi_Email.Email_pec=0 and st1Indirizzi_Email.Email_chiave_id = Indirizzi_Email.Email_chiave_id 
		ORDER BY st1Indirizzi_Email.Email_indirizzo FOR XML PATH ('')), 3, 1000) [Email] 
	FROM Indirizzi_Email Indirizzi_Email where Indirizzi_Email.Email_tabella=3 and Indirizzi_Email.Email_chiave_id=Amm_id),''))  as Amm_email,

	(SELECT coalesce((SELECT DISTINCT SUBSTRING((SELECT '; ' + Email_indirizzo AS [text()] 
		FROM Indirizzi_Email st1Indirizzi_Email 
		WHERE st1Indirizzi_Email.Email_tabella=3 and st1Indirizzi_Email.Email_pec=1 and st1Indirizzi_Email.Email_chiave_id = Indirizzi_Email.Email_chiave_id 
		ORDER BY st1Indirizzi_Email.Email_indirizzo FOR XML PATH ('')), 3, 1000) [Email] 
	FROM Indirizzi_Email Indirizzi_Email where Indirizzi_Email.Email_tabella=3 and Indirizzi_Email.Email_chiave_id=Amm_id),''))  as Amm_PEC,

 	Pre_descr_commessa ,coalesce(format(Pre_data_fine_lavori, 'dd/MM/yyyy'),'') as FineLavori, 
	iif(pre_stato_doc=4,'Chiusa',iif(pre_stato_doc=5,'Sospesa','Aperta')) as StatoCommessa,
	iif(Pre_fatturato=1,'Fatturato',iif((select SUM(PREF_IMPONIBILE) from Preventivi_Fatture where PreF_preventivo=Pre_id)>0,'Acconto','Da fatturare')) as StatoFatturaz,
						 Pre_note as Note

from Preventivi_Commesse left join Preventivi_Impianti on Preventivi_Commesse.Pre_id = Preventivi_Impianti.PreI_preventivo left join
	Impianti on Preventivi_Impianti.PreI_impianto = impianti.Imp_codice left join
	Amministratori on Impianti.Imp_amministratore=Amministratori.Amm_id
where Pre_anno_commessa<>0 and Pre_stato_doc <=5 and pre_societa = 1