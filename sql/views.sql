USE personnal_finance_db;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `transactions_monthly_details` AS
WITH transactions_data AS (
	SELECT  
        t.account_id,
		t.date,
        LAST_DAY(t.date) AS month_last_day,  -- On récupère le dernier jour du mois pour regrouper par mois
		t.amount,
		t.is_expense,
        
        -- Attribution de la catégorie : si c'est un retrait, on force "Retrait", sinon on prend la catégorie taguée par l'utilisateur
        -- ou celle associée au payee, et par défaut "Non catégorisé"
		CASE 
			WHEN pm.name = 'RETRAIT' THEN (SELECT id FROM categories WHERE name = 'Retrait')
			ELSE COALESCE(c1.id, c2.id, (SELECT id FROM categories WHERE name = 'Non catégorisé')) 
		END AS category_id,
        
        -- Même logique pour la catégorie parente, mais on inclut aussi la catégorie associée au payee si aucune autre n'est disponible
		CASE 
			WHEN pm.name = 'RETRAIT' THEN (SELECT id FROM categories WHERE name = 'Retrait')
			ELSE COALESCE(pc1.id, pc2.id, c2.id, (SELECT id FROM categories WHERE name = 'Non catégorisé')) 
		END AS parent_category_id,
        
		pm.id AS payment_method_id
	FROM personnal_finance_db.transactions t
	-- Lien transaction -> moyen de paiement via motif du mémo
	INNER JOIN personnal_finance_db.payment_methods_transaction_link pmt 
        ON t.memo LIKE pmt.transaction_memo_pattern
	INNER JOIN personnal_finance_db.payment_methods pm 
        ON pmt.payment_method_id = pm.id
	-- Récupération de la catégorie associée au payee si la transaction est une dépense
	LEFT JOIN personnal_finance_db.categories_transaction_link ctl 
        ON t.clean_payee = ctl.payee AND t.is_expense = ctl.is_expense
	-- Catégorie manuelle de l'utilisateur
	LEFT JOIN personnal_finance_db.categories c1 
        ON t.user_tag_category_id = c1.id
	LEFT JOIN personnal_finance_db.categories pc1 
        ON c1.parent_id = pc1.id
	-- Catégorie via payee
	LEFT JOIN personnal_finance_db.categories c2 
        ON ctl.category_id = c2.id
	LEFT JOIN personnal_finance_db.categories pc2 
        ON c2.parent_id = pc2.id
	WHERE t.user_ignore_transaction = 0  -- On ignore les transactions marquées comme ignorées
)
SELECT 
	account_id, 
    month_last_day,
    DATE_FORMAT(month_last_day, '%Y-%m-01') AS month_first_day,  -- On calcule le premier jour du mois pour faciliter les rapports
    SUM(amount) AS amount,  -- Total des montants par regroupement
    is_expense, 
    category_id, 
    parent_category_id, 
    payment_method_id
FROM transactions_data
-- Agrégation par compte, mois, type de transaction et catégories
GROUP BY account_id, month_last_day, is_expense, category_id, parent_category_id, payment_method_id;


CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `planned_transactions_history` AS
WITH RECURSIVE all_planned_transactions AS (
    -- Génération de toutes les occurrences de transactions planifiées selon leur fréquence
    SELECT 
        pt.id AS id,
        pt.is_expense AS is_expense,
        pt.amount AS due_amount,
        pt.start_date AS start_date,
        COALESCE(pt.end_date, CAST(NOW() AS DATE)) AS end_date, -- Si pas de date de fin, on prend aujourd'hui
        pt.frequency AS frequency,
        pt.payee AS payee,
        LAST_DAY(pt.start_date) AS month_last_day  -- Dernier jour du mois de départ
    FROM planned_transactions pt

    UNION ALL

    -- Création des occurrences suivantes selon la fréquence (mensuelle ou annuelle)
    SELECT 
        apt.id,
        apt.is_expense,
        apt.due_amount,
        apt.start_date,
        apt.end_date,
        apt.frequency,
        apt.payee,
        CASE 
            WHEN apt.frequency = 'monthly' 
                THEN LAST_DAY(apt.month_last_day + INTERVAL 1 MONTH)  -- Mois suivant
            ELSE LAST_DAY(apt.month_last_day + INTERVAL 1 YEAR)     -- Année suivante
        END AS month_last_day
    FROM all_planned_transactions apt
    WHERE apt.month_last_day < apt.end_date  -- On s'arrête à la date de fin
),

all_planned_transactions_details AS (
    -- Calcul du montant total dû cumulatif pour chaque transaction planifiée
    SELECT 
        apt.id,
        apt.is_expense,
        apt.due_amount,
        SUM(apt.due_amount) OVER (
            PARTITION BY apt.id 
            ORDER BY apt.month_last_day
        ) AS total_due_amount,
        apt.start_date,
        apt.end_date,
        apt.frequency,
        apt.payee,
        apt.month_last_day
    FROM all_planned_transactions apt
    ORDER BY apt.payee, apt.month_last_day
),

planned_transactions_paid AS (
    -- Identification des transactions réelles correspondant aux transactions planifiées
    SELECT 
        pt.id,
        t.id AS transaction_id,
        t.is_expense,
        t.amount AS paid_amount,
        LAST_DAY(t.date) AS month_last_day,
        t.date,
        ROW_NUMBER() OVER (
            PARTITION BY t.id 
            ORDER BY pt.end_date
        ) AS row_id  -- Pour ne garder qu'une correspondance principale par transaction
    FROM planned_transactions pt
    JOIN transactions t 
        ON pt.is_expense = t.is_expense
        AND pt.payee = t.clean_payee
        AND pt.start_date <= t.date
        AND COALESCE(pt.end_date + INTERVAL 1 MONTH, NOW()) >= t.date
),

amount_planned_transactions_paid AS (
    -- Calcul du montant payé cumulatif pour chaque transaction planifiée
    SELECT 
        ptp.id,
        ptp.transaction_id,
        ptp.is_expense,
        ptp.paid_amount,
        SUM(ptp.paid_amount) OVER (
            PARTITION BY ptp.id 
            ORDER BY ptp.date
        ) AS total_paid_amount,
        LAST_DAY(ptp.date) AS month_last_day
    FROM planned_transactions_paid ptp
    WHERE ptp.row_id = 1  -- On ne garde que la première correspondance par transaction
),

linked_due_paid_planned_transactions AS (
    -- Lien entre montants dus et montants payés pour chaque occurrence
    SELECT 
        d.id,
        d.is_expense,
        d.due_amount,
        p.paid_amount,
        d.total_due_amount,
        p.total_paid_amount,
        d.payee,
        d.month_last_day AS due_month_last_day,
        p.month_last_day AS paid_month_last_day,
        ROW_NUMBER() OVER (
            PARTITION BY d.id, d.month_last_day 
            ORDER BY COALESCE(p.month_last_day, NOW())  -- Priorité aux paiements effectués, sinon date actuelle
        ) AS row_id
    FROM all_planned_transactions_details d
    LEFT JOIN amount_planned_transactions_paid p 
        ON d.id = p.id
        AND d.total_due_amount <= p.total_paid_amount
)

-- Résultat final avec le statut de paiement
SELECT 
    l.is_expense,
    l.due_amount,
    l.paid_amount,
    l.total_due_amount,
    l.total_paid_amount,
    l.payee,
    l.due_month_last_day,
    l.paid_month_last_day,
    CASE 
        WHEN l.paid_month_last_day IS NOT NULL 
             AND l.paid_month_last_day = l.due_month_last_day 
            THEN 'Payé'  -- Paiement effectué dans le même mois que prévu
        WHEN l.paid_month_last_day IS NOT NULL 
             AND l.paid_month_last_day <> l.due_month_last_day 
            THEN CONCAT(
                'Payé en ', 
                RIGHT(CONCAT('0', MONTH(l.paid_month_last_day)), 2),
                '/', YEAR(l.paid_month_last_day)
            )  -- Paiement effectué mais décalé
        ELSE 'En attente'  -- Pas encore payé
    END AS status
FROM linked_due_paid_planned_transactions l
WHERE l.row_id = 1  -- On ne garde que la correspondance principale par occurrence
ORDER BY l.payee, l.due_month_last_day;


CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `amortization_table` AS
/* ======================================================================
   Génération d'un tableau d'amortissement avec période "avant 1ʳᵉ échéance"
   puis période d'amortissement mensuel (annuités constantes).

   Hypothèses clés :
   - Les intérêts courus AVANT la first_installment_date sont calculés
     à l'année / prorata et NE S'AFFICHENT que le mois de la 1ʳᵉ échéance.
   - Après la 1ʳᵉ échéance : amortissement avec mensualité constante.

   NOTE : on conserve ta logique/structure telle quelle.
   ====================================================================== */

WITH RECURSIVE initial_month AS (
	/* --------------------------------------------------------------
	   Point de départ : on récupère les infos du prêt et on pose
	   la "fin de mois" pour itérer mois par mois.
	   -------------------------------------------------------------- */
	SELECT 
        l.id,
        l.amount AS amount_loaned,              -- Montant emprunté (capital initial)
        l.flat_rate_value,                      -- Taux annuel (ex: 0.05 pour 5%)
        l.subscription_date,                    -- Date de souscription
        l.first_installment_date,               -- Date de 1ʳᵉ échéance
        LAST_DAY(l.subscription_date) AS month_last_day,  -- Dernier jour du mois de souscription
        l.total_month_duration                  -- Durée totale (en mois)
	FROM loan l
),

amortization_before_first_installment AS (
	/* --------------------------------------------------------------
	   Cumule des intérêts avant la 1ʳᵉ échéance, par "blocs" annuels.
	   - month_duration : compteur en mois pour piloter les recursions.
	   - start_year_total_due_amount : capital dû en début d'année courante.
	   - interest : intérêts de l'année (ou fraction initiale).
	   - amount_repaid : 0 avant la 1ʳᵉ échéance (règle métier).
	   - end_year_total_due_amount : capital dû fin d'année courante.
	   -------------------------------------------------------------- */

	-- 1) Première "année" partielle : depuis la souscription jusqu'à la 1ʳᵉ échéance
	SELECT
		id,
        amount_loaned,
        flat_rate_value,
        subscription_date,
        first_installment_date,
        month_last_day,
        total_month_duration,
        1 AS month_duration,  -- On démarre le compteur

        amount_loaned AS start_year_total_due_amount,  -- Capital dû en début de période "avant 1ʳᵉ échéance"

        -- Intérêts "avant 1ʳᵉ échéance" = fraction du 1er mois + mois entiers
        -- Fraction initiale : du jour de souscription au "même jour" du mois
        -- de la 1ʳᵉ échéance (dans le mois de souscription)
        (DATEDIFF(
			DATE_FORMAT(subscription_date, CONCAT('%Y-%m-', DAY(first_installment_date))), subscription_date
		) / DAY(LAST_DAY(subscription_date))) * amount_loaned * flat_rate_value / 12
        + 11 * (amount_loaned * flat_rate_value / 12)
        AS interest,

        0 AS amount_repaid,  -- Aucun remboursement avant la 1ʳᵉ échéance

        -- Capital dû fin "année" : capital initial + intérêts cumulés "avant 1ʳᵉ échéance"
        -- (on garde ta formule telle quelle)
        amount_loaned
        + (DATEDIFF(
			DATE_FORMAT(subscription_date, CONCAT('%Y-%m-', DAY(first_installment_date))), subscription_date
		  ) / DAY(LAST_DAY(subscription_date))) * amount_loaned * flat_rate_value / 12
        + 11 * (amount_loaned * flat_rate_value / 12)  -- bloc annuel (conservé tel quel)
        AS end_year_total_due_amount
	FROM initial_month
    
    UNION ALL
    
    -- 2) Années pleines ultérieures avant la 1ʳᵉ échéance (si la 1ʳᵉ échéance est à + de 12 mois)
    SELECT
		id,
        amount_loaned,
        flat_rate_value,
        subscription_date,
        first_installment_date,
        LAST_DAY(DATE_ADD(month_last_day, INTERVAL 1 YEAR)) AS month_last_day,  -- On avance d'un an (fin de mois)
        total_month_duration,
        month_duration + 12 AS month_duration,  -- On ajoute 12 mois

        end_year_total_due_amount AS start_year_total_due_amount,  -- Nouveau départ = fin d'année précédente
        end_year_total_due_amount * flat_rate_value AS interest,   -- Intérêts d'une année pleine
        0 AS amount_repaid,                                        -- Toujours 0 avant 1ʳᵉ échéance
        end_year_total_due_amount + end_year_total_due_amount * flat_rate_value AS end_year_total_due_amount
	FROM amortization_before_first_installment
    WHERE DATE_ADD(month_last_day, INTERVAL 1 YEAR) < first_installment_date  -- Tant qu'on est avant la 1ʳᵉ échéance
),

monthly_amortization_before_first_installment AS(
	/* --------------------------------------------------------------
	   Dépliage MENSUEL de la période "avant 1ʳᵉ échéance".
	   On projette chaque mois, sans remboursement, pour afficher
	   l'état mois par mois jusqu'au mois de la 1ʳᵉ échéance.
	   -------------------------------------------------------------- */

	-- 1) Première ligne mensuelle : on distribue l'intérêt annuel par 12 pour affichage mensuel
	SELECT
		a.id,
        amount_loaned,
        flat_rate_value,
        subscription_date,
        first_installment_date,
        month_last_day,
        total_month_duration,
        month_duration,
        start_year_total_due_amount AS start_month_total_due_amount,  -- Montant dû en début de mois
        interest / 12 AS interest,                                    -- On lisse l'intérêt annuel sur 12 pour affichage
        COALESCE(lep.amount, 0) AS month_due_amount,                  -- Aucune mensualité due avant 1ʳᵉ échéance
        amount_repaid / 12 AS amount_repaid,                          -- 0/12 = 0 (cohérent)
        end_year_total_due_amount,                                    -- Mémoire pour le basculement d'année
        start_year_total_due_amount - COALESCE(lep.amount, 0) AS end_month_total_due_amount     -- Pas d'évolution du dû mensuel avant échéance
	FROM amortization_before_first_installment a
    LEFT JOIN loan_early_payments lep ON a.id = lep.loan_id AND a.month_last_day = LAST_DAY(lep.date)
    
    UNION ALL
    
    -- 2) Mois suivants avant la 1ʳᵉ échéance (jusqu’au mois précédent)
    SELECT
		a.id,
        amount_loaned,
        flat_rate_value,
        subscription_date,
        first_installment_date,
        LAST_DAY(DATE_ADD(month_last_day, INTERVAL 1 MONTH)) AS month_last_day,  -- On passe au mois suivant (fin de mois)
        total_month_duration,
        month_duration + 1 AS month_duration,
        start_month_total_due_amount,           -- Reste identique (pas de remboursement)
        interest,                               -- Affichage de la part d'intérêt mensuelle
        month_due_amount,                       -- Toujours 0 avant la 1ʳᵉ échéance
        amount_repaid + COALESCE(lep.amount, 0),-- 0
        end_year_total_due_amount,              -- À utiliser quand on franchit une "année"
        CASE
			-- Si le mois suivant n'appartient pas à une nouvelle "année" de la CTE annuelle,
            -- on garde le même "end_month_total_due_amount".
			WHEN LAST_DAY(DATE_ADD(month_last_day, INTERVAL 2 MONTH)) NOT IN (SELECT month_last_day FROM amortization_before_first_installment)
            THEN start_month_total_due_amount
            -- Sinon, on bascule sur la valeur de fin d'année pré-calculée.
            ELSE end_year_total_due_amount
		END - COALESCE(lep.amount, 0) AS end_month_total_due_amount
	FROM monthly_amortization_before_first_installment a
    LEFT JOIN loan_early_payments lep ON a.id = lep.loan_id AND LAST_DAY(DATE_ADD(a.month_last_day, INTERVAL 1 MONTH)) = LAST_DAY(lep.date)
    WHERE DATE_ADD(month_last_day, INTERVAL 1 MONTH) < first_installment_date  -- On s'arrête le mois de la 1ʳᵉ échéance
      AND LAST_DAY(DATE_ADD(month_last_day, INTERVAL 1 MONTH)) NOT IN (SELECT month_last_day FROM amortization_before_first_installment)
),

monthly_amortization_after_first_installment AS (
	/* --------------------------------------------------------------
	   Après la 1ʳᵉ échéance : amortissement classique "méthode française"
	   (mensualité constante).
	   - month_due_amount : mensualité constante calculée à partir du capital restant.
	   - interest : intérêt du mois (selon logique conservée).
	   - amount_repaid : part de capital remboursée ce mois.
	   - end_month_total_due_amount : CRD en fin de mois.
	   -------------------------------------------------------------- */

	-- 1) Première ligne post-1ʳᵉ échéance : on part du dernier état "avant échéance"
	SELECT
		a.id,
        amount_loaned,
        flat_rate_value,
        subscription_date,
        first_installment_date,
        LAST_DAY(DATE_ADD(month_last_day, INTERVAL 1 MONTH)) AS month_last_day,  -- Mois de la 1ʳᵉ échéance
        total_month_duration,
        month_duration + 1 AS month_duration,
        end_month_total_due_amount AS start_month_total_due_amount,               -- CRD de départ
        end_month_total_due_amount * flat_rate_value AS interest,                -- Intérêts (formule conservée)
        ((end_month_total_due_amount * flat_rate_value)/12)
          / (1 - POWER(1 + flat_rate_value / 12, -(total_month_duration - month_duration))) AS month_due_amount,  -- Mensualité
        ((end_month_total_due_amount * flat_rate_value)/12)
          / (1 - POWER(1 + flat_rate_value / 12, -(total_month_duration - month_duration)))
          - end_month_total_due_amount * flat_rate_value / 12 + COALESCE(lep.amount, 0) AS amount_repaid,  -- Part de capital remboursée
        end_month_total_due_amount + end_month_total_due_amount * flat_rate_value - COALESCE(lep.amount, 0)
          - ((end_month_total_due_amount * flat_rate_value)/12)
              / (1 - POWER(1 + flat_rate_value / 12, -(total_month_duration - month_duration)))
          AS end_month_total_due_amount                                            -- Nouveau CRD
	FROM monthly_amortization_before_first_installment a
    LEFT JOIN loan_early_payments lep ON a.id = lep.loan_id AND LAST_DAY(DATE_ADD(a.month_last_day, INTERVAL 1 MONTH)) = LAST_DAY(lep.date)
    WHERE month_duration = (SELECT MAX(month_duration) FROM monthly_amortization_before_first_installment)  -- On prend la dernière ligne "avant"

    UNION ALL

    -- 2) Mois suivants jusqu’à la fin de la durée totale
    SELECT
		a.id,
        amount_loaned,
        flat_rate_value,
        subscription_date,
        first_installment_date,
        LAST_DAY(DATE_ADD(month_last_day, INTERVAL 1 MONTH)) AS month_last_day,   -- Mois suivant
        total_month_duration,
        month_duration + 1 AS month_duration,
        end_month_total_due_amount AS start_month_total_due_amount,               -- CRD porté
        end_month_total_due_amount * flat_rate_value / 12 AS interest,            -- Intérêts mensuels
        ((end_month_total_due_amount * flat_rate_value)/12)
          / (1 - POWER(1 + flat_rate_value / 12, -(total_month_duration - month_duration))) AS month_due_amount,
        ((end_month_total_due_amount * flat_rate_value)/12)
          / (1 - POWER(1 + flat_rate_value / 12, -(total_month_duration - month_duration)))
          - end_month_total_due_amount * flat_rate_value / 12 + COALESCE(lep.amount, 0) AS amount_repaid,
        end_month_total_due_amount + end_month_total_due_amount * flat_rate_value / 12 - COALESCE(lep.amount, 0)
          - ((end_month_total_due_amount * flat_rate_value)/12)
              / (1 - POWER(1 + flat_rate_value / 12, -(total_month_duration - month_duration)))
          AS end_month_total_due_amount
	FROM monthly_amortization_after_first_installment a
    LEFT JOIN loan_early_payments lep ON a.id = lep.loan_id AND LAST_DAY(DATE_ADD(a.month_last_day, INTERVAL 1 MONTH)) = LAST_DAY(lep.date)
    WHERE month_duration < total_month_duration  -- On s'arrête à la durée totale
),

amortization_table AS (
	/* --------------------------------------------------------------
	   Union des lignes "avant 1ʳᵉ échéance" (mensuelles) et
	   "après 1ʳᵉ échéance".
	   On garde seulement les colonnes nécessaires pour l'affichage final.
	   -------------------------------------------------------------- */
	SELECT
		id,
        amount_loaned,
        flat_rate_value,
        subscription_date,
        first_installment_date,
        month_last_day,
        total_month_duration,
        month_duration,
        start_month_total_due_amount,
        interest,
        month_due_amount,
        amount_repaid,
        end_month_total_due_amount
	FROM monthly_amortization_before_first_installment

    UNION

	SELECT
		id,
        amount_loaned,
        flat_rate_value,
        subscription_date,
        first_installment_date,
        month_last_day,
        total_month_duration,
        month_duration,
        start_month_total_due_amount,
        interest,
        month_due_amount,
        amount_repaid,
        end_month_total_due_amount
	FROM monthly_amortization_after_first_installment
)

-- ======================= Résultat final ===========================
SELECT 
	id,
	amount_loaned,
	flat_rate_value,
	subscription_date,
	first_installment_date,
	month_last_day,                 -- Date fin de mois (jalon mensuel)
	total_month_duration,           -- Durée totale du prêt (mois)
	month_duration,                 -- Compteur de mois depuis le début
	ROUND(start_month_total_due_amount, 4) AS start_month_total_due_amount,  -- CRD début de mois
	ROUND(interest, 4) AS interest,                                          -- Intérêt "affiché" pour le mois
	ROUND(month_due_amount, 4) AS month_due_amount,                          -- Mensualité totale (si > 0 après 1ʳᵉ échéance)
	ROUND(amount_repaid, 4) AS amount_repaid,                                -- Part de capital remboursé ce mois
	ROUND(end_month_total_due_amount, 4) AS end_month_total_due_amount       -- CRD fin de mois
FROM amortization_table;


CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `securities_wallet_evolution` AS
-- Étape 1 : calcul des quantités cumulées détenues par ISIN
WITH wallet_qty_securities AS (
	SELECT
		date,
		isin,
		operation_type,
        account_id,
		-- On cumule les quantités achetées (positives) et vendues (négatives)
		SUM(
			CASE
				WHEN operation_type = 'purchase' THEN quantity
				ELSE -quantity
			END 
		) OVER(PARTITION BY isin, account_id ORDER BY date) AS total_quantity
	FROM security_operations
	WHERE operation_type <> 'tax' -- on exclut les opérations fiscales
),

-- Étape 2 : calcul du coût d’acquisition cumulé et du PRU (prix de revient unitaire)
wallet_price_securities AS (
	SELECT
		date,
		isin,
		-- Montant cumulé investi sur ce titre
		SUM(quantity * net_unit_price) OVER(PARTITION BY isin ORDER BY date) AS net_amount,
		-- Prix de revient unitaire moyen (PRU)
		SUM(quantity * net_unit_price) OVER(PARTITION BY isin ORDER BY date)
		/ SUM(quantity) OVER(PARTITION BY isin ORDER BY date) AS net_unit_price
	FROM security_operations
	WHERE operation_type = 'purchase'
),

-- Étape 3 : évolution du portefeuille par titre
wallet_securities_evolution AS (
	SELECT
		sp.date,
        wqs.account_id,
		wqs.isin,
		wqs.total_quantity, -- quantité détenue à cette date
		wps.net_unit_price AS unit_cost_price, -- PRU
		wqs.total_quantity * wps.net_unit_price AS cost_price, -- coût total de la position
		sp.open_price,
		sp.close_price,
		wqs.total_quantity * sp.close_price AS capital_value, -- valeur de marché de la position
		wqs.total_quantity * sp.close_price - wqs.total_quantity * wps.net_unit_price AS capital_gain_value, -- plus-value latente
		sp.close_price / wps.net_unit_price - 1 AS capital_gain_rate, -- taux de rendement
		ROW_NUMBER() OVER(PARTITION BY wqs.isin, sp.date ORDER BY wqs.date DESC) AS row_id
	FROM wallet_qty_securities wqs
	LEFT JOIN wallet_price_securities wps 
		ON wqs.isin = wps.isin AND wqs.date = wps.date
	LEFT JOIN wallet_qty_securities sold 
		ON wqs.isin = sold.isin AND wqs.account_id = sold.account_id AND sold.total_quantity = 0 -- permet d’arrêter le suivi après une liquidation totale
	LEFT JOIN security_prices sp 
		ON wqs.isin = sp.isin 
		AND wqs.date <= sp.date 
		AND COALESCE(sold.date, NOW()) >= sp.date -- borne max = date de vente totale (ou NOW si encore détenu)
	WHERE wqs.total_quantity > 0 -- on ne garde que les titres encore détenus
),

-- Étape 4 : calcul du rendement global pondéré par le capital investi
wallet_securities_evolution_global_rate AS (
	SELECT
		date,
		-- pondération par le coût investi sur chaque titre
		SUM(cost_price * capital_gain_rate) / NULLIF(SUM(cost_price), 0) AS global_gain_rate
	FROM wallet_securities_evolution
	WHERE row_id = 1 -- évite les doublons sur la même date
	GROUP BY date
),

-- Étape 5 : variation journalière du rendement global
wallet_securities_evolution_global AS (
	SELECT 
		date,
		global_gain_rate,
		-- différence avec le jour précédent (0 si première date)
		COALESCE(global_gain_rate - LAG(global_gain_rate, 1) OVER (ORDER BY date), 0) AS global_gain_rate_since_yesterday
	FROM wallet_securities_evolution_global_rate
)

-- Résultat final : détails par titre + agrégation globale
SELECT
	e.date,
    e.account_id,
	e.isin,
    s.name,
    s.type,
	e.total_quantity,
	ROUND(e.cost_price, 4) AS cost_price,
	ROUND(e.unit_cost_price, 4) AS unit_cost_price,
	ROUND(e.open_price, 4) AS open_price,
	ROUND(e.close_price, 4) AS close_price,
	ROUND(e.capital_value, 4) AS capital_value,
	ROUND(e.capital_value / SUM(e.capital_value) OVER(PARTITION BY e.date), 8) AS capital_rate,
	ROUND(e.capital_gain_value, 4) AS capital_gain_value,
	ROUND(e.capital_gain_rate, 8) AS capital_gain_rate,
	ROUND(g.global_gain_rate, 8) AS global_gain_rate,
	ROUND(g.global_gain_rate_since_yesterday, 8) AS global_gain_rate_since_yesterday
FROM wallet_securities_evolution e
INNER JOIN wallet_securities_evolution_global g ON e.date = g.date
INNER JOIN securities s ON e.isin = s.isin
WHERE e.row_id = 1
ORDER BY date, isin;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `balance_history` AS
-- Étape 1 : récupérer le dernier solde connu pour chaque compte
WITH RECURSIVE last_balance_update AS (
	SELECT 	
        account_id, 
		LAST_DAY(date) AS month_last_day,   -- fin du mois correspondant
		value,                               -- valeur du solde
		-- Numérotation pour identifier la dernière valeur par compte
		ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY date DESC) AS row_id
	FROM balances
),

-- Étape 2 : calcul des flux de trésorerie mensuels par compte
cash_flow AS (
	SELECT
        account_id,
		LAST_DAY(date) AS month_last_day,  -- fin du mois
		SUM(
			CASE
				WHEN is_expense THEN -amount   -- dépenses = négatif
				ELSE amount                   -- revenus = positif
			END
		) AS cash_flow
	FROM transactions
	GROUP BY account_id, LAST_DAY(date)
),

-- Étape 3 : génération d’une série mensuelle inversée à partir du dernier solde
month_serie AS (
	SELECT
        account_id, 
		month_last_day,
        value
	FROM last_balance_update
    WHERE row_id = 1                    -- on prend uniquement le dernier solde connu
	UNION ALL
	SELECT
        account_id, 
		LAST_DAY(DATE_SUB(month_last_day, INTERVAL 1 MONTH)),  -- mois précédent
        value
	FROM month_serie
    WHERE month_last_day > (SELECT MIN(date) FROM transactions)  -- on arrête à la première transaction
),

-- Étape 4 : récupérer le dernier jour de cotation pour chaque mois
last_stock_market_days AS (
	SELECT
		LAST_DAY(date) AS month_last_day,     -- fin du mois
		MAX(date) AS month_last_stock_market_day  -- dernier jour de cotation
	FROM security_prices
	GROUP BY LAST_DAY(date)
),

-- Étape 5 : calculer la valeur mensuelle des titres détenus
monthly_stock_market_value AS (
	SELECT 
		pswe.account_id,
		lsmd.month_last_day,
		SUM(pswe.capital_value) AS value  -- valeur totale des titres
	FROM securities_wallet_evolution pswe
	INNER JOIN last_stock_market_days lsmd 
        ON pswe.date = lsmd.month_last_stock_market_day
	GROUP BY pswe.account_id, lsmd.month_last_day
),

-- Étape 6 : reconstituer l'historique des soldes
balance_history AS (
	SELECT DISTINCT
		ms.account_id,
		ms.month_last_day,
		-- valeur corrigée des flux : solde initial - flux cumulés après ce mois
		ms.value - COALESCE(SUM(cf.cash_flow) OVER(PARTITION BY account_id, month_last_day), 0) AS value
	FROM month_serie ms
	LEFT JOIN cash_flow cf 
        ON ms.account_id = cf.account_id AND ms.month_last_day < cf.month_last_day
	UNION
	SELECT
        account_id,
		month_last_day,
		value
	FROM monthly_stock_market_value
	WHERE month_last_day > (SELECT MIN(date) FROM transactions)
)

-- Résultat final : historique mensuel du solde avec début et fin de mois
SELECT
	account_id,
    month_last_day,
    DATE_FORMAT(month_last_day, '%Y-%m-01') AS month_first_day,
    value
FROM balance_history;


CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `global_finance_data_15_month` AS
WITH 
-- Valeur brute des actifs par mois
monthly_asset_value AS (
    SELECT 
        bh.month_last_day,
        bh.month_first_day,
        SUM(bh.value) AS gross_asset_value
    FROM balance_history bh
    GROUP BY bh.month_last_day, bh.month_first_day
),

-- Flux de trésorerie mensuel (entrées/sorties)
monthly_cash_flow AS (
    SELECT 
        t.month_last_day,
        t.is_expense,
        SUM(t.amount) AS amount
    FROM transactions_monthly_details t
    JOIN accounts a ON t.account_id = a.id
    JOIN account_type at ON a.account_type_id = at.id
    JOIN categories c ON t.parent_category_id = c.id
    WHERE at.is_checking_account <> 0
      AND c.name <> 'Virements internes'
      AND c.name <> 'Epargne'
    GROUP BY t.month_last_day, t.is_expense
)

-- Sélection finale
SELECT 
    m.month_last_day,
    m.month_first_day,
    m.gross_asset_value,
    a.end_month_total_due_amount AS loaned_amount,
    ROUND(m.gross_asset_value - a.end_month_total_due_amount, 2) AS net_asset_value,
    credit.amount AS credit_value,
    expense.amount AS expense_value,
    (credit.amount - expense.amount) AS saving_value,
    ((credit.amount - expense.amount) / credit.amount) AS saving_rate
FROM monthly_asset_value m
LEFT JOIN amortization_table a 
       ON m.month_last_day = a.month_last_day
LEFT JOIN monthly_cash_flow credit 
       ON m.month_last_day = credit.month_last_day 
      AND credit.is_expense = 0
LEFT JOIN monthly_cash_flow expense 
       ON m.month_last_day = expense.month_last_day 
      AND expense.is_expense = 1
WHERE m.month_last_day > LAST_DAY(NOW() - INTERVAL 15 MONTH);
