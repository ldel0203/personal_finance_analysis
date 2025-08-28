-- Vue : Détails mensuels des transactions (agrégation par compte / mois / catégorie / méthode de paiement)
CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `transactions_monthly_details` AS
WITH transactions_data AS (
    SELECT  
        t.account_id,
        t.date,
        LAST_DAY(t.date) AS month_last_day, -- fin de mois (clé d’agrégation)
        t.amount,
        t.is_expense,
        -- Détermination de la catégorie (Retrait, catégorie trouvée, ou "Non catégorisé")
        CASE 
            WHEN pm.name = 'RETRAIT' THEN (SELECT id FROM categories WHERE name = 'Retrait')
            ELSE COALESCE(c1.id, c2.id, (SELECT id FROM categories WHERE name = 'Non catégorisé')) 
        END AS category_id,
        -- Détermination de la catégorie parente
        CASE 
            WHEN pm.name = 'RETRAIT' THEN (SELECT id FROM categories WHERE name = 'Retrait')
            ELSE COALESCE(pc1.id, pc2.id, c2.id, (SELECT id FROM categories WHERE name = 'Non catégorisé')) 
        END AS parent_category_id,
        pm.id AS payment_method_id
    FROM personnal_finance_db.transactions t
    INNER JOIN personnal_finance_db.payment_methods_transaction_link pmt 
        ON t.memo LIKE pmt.transaction_memo_pattern
    INNER JOIN personnal_finance_db.payment_methods pm 
        ON pmt.payment_method_id = pm.id
    LEFT JOIN personnal_finance_db.categories_transaction_link ctl 
        ON t.clean_payee = ctl.payee AND t.is_expense = ctl.is_expense
    LEFT JOIN personnal_finance_db.categories c1 ON t.user_tag_category_id = c1.id
    LEFT JOIN personnal_finance_db.categories pc1 ON c1.parent_id = pc1.id
    LEFT JOIN personnal_finance_db.categories c2 ON ctl.category_id = c2.id
    LEFT JOIN personnal_finance_db.categories pc2 ON c2.parent_id = pc2.id
    WHERE t.user_ignore_transaction = 0 -- On exclut les transactions ignorées
)
SELECT 
    account_id, 
    month_last_day,
    DATE_FORMAT(month_last_day, '%Y-%m-01') AS month_first_day, -- début du mois
    SUM(amount) AS amount, 
    is_expense, 
    category_id, 
    parent_category_id, 
    payment_method_id
FROM transactions_data
GROUP BY account_id, month_last_day, is_expense, category_id, parent_category_id, payment_method_id;


-- Vue : Historique des transactions planifiées (prévu vs payé)
CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `planned_transactions_history` AS
WITH RECURSIVE all_planned_transactions AS (
    -- Étend chaque transaction planifiée en multiples lignes mensuelles/annuelles
    SELECT 
        pt.id,
        pt.is_expense,
        pt.amount AS due_amount,
        pt.start_date,
        COALESCE(pt.end_date, CAST(NOW() AS DATE)) AS end_date,
        pt.frequency,
        pt.payee,
        LAST_DAY(pt.start_date) AS month_last_day
    FROM planned_transactions pt

    UNION ALL

    SELECT 
        apt.id,
        apt.is_expense,
        apt.due_amount,
        apt.start_date,
        apt.end_date,
        apt.frequency,
        apt.payee,
        -- Ajout d’une nouvelle échéance (mois suivant ou année suivante selon la fréquence)
        CASE 
            WHEN apt.frequency = 'monthly' 
                THEN LAST_DAY(apt.month_last_day + INTERVAL 1 MONTH)
            ELSE LAST_DAY(apt.month_last_day + INTERVAL 1 YEAR)
        END
    FROM all_planned_transactions apt
    WHERE apt.month_last_day < apt.end_date
),

all_planned_transactions_details AS (
    -- Ajoute le cumul des montants dus pour chaque transaction planifiée
    SELECT 
        apt.id,
        apt.is_expense,
        apt.due_amount,
        SUM(apt.due_amount) OVER (PARTITION BY apt.id ORDER BY apt.month_last_day) AS total_due_amount,
        apt.start_date,
        apt.end_date,
        apt.frequency,
        apt.payee,
        apt.month_last_day
    FROM all_planned_transactions apt
    ORDER BY apt.payee, apt.month_last_day
),

planned_transactions_paid AS (
    -- Associe les transactions réelles aux transactions planifiées correspondantes
    SELECT 
        pt.id,
        t.id AS transaction_id,
        t.is_expense,
        t.amount AS paid_amount,
        LAST_DAY(t.date) AS month_last_day,
        t.date,
        ROW_NUMBER() OVER (PARTITION BY t.id ORDER BY pt.end_date) AS row_id
    FROM planned_transactions pt
    JOIN transactions t 
        ON pt.is_expense = t.is_expense
        AND pt.payee = t.clean_payee
        AND pt.start_date <= t.date
        AND COALESCE(pt.end_date + INTERVAL 1 MONTH, NOW()) >= t.date
),

amount_planned_transactions_paid AS (
    -- Ajoute cumul payé pour chaque transaction planifiée
    SELECT 
        ptp.id,
        ptp.transaction_id,
        ptp.is_expense,
        ptp.paid_amount,
        SUM(ptp.paid_amount) OVER (PARTITION BY ptp.id ORDER BY ptp.date) AS total_paid_amount,
        LAST_DAY(ptp.date) AS month_last_day
    FROM planned_transactions_paid ptp
    WHERE ptp.row_id = 1
),

linked_due_paid_planned_transactions AS (
    -- Associe "prévu" et "réalisé" pour comparer l’état d’avancement
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
        ROW_NUMBER() OVER (PARTITION BY d.id, d.month_last_day ORDER BY COALESCE(p.month_last_day, NOW())) AS row_id
    FROM all_planned_transactions_details d
    LEFT JOIN amount_planned_transactions_paid p 
        ON d.id = p.id
        AND d.total_due_amount <= p.total_paid_amount
)

SELECT 
    l.is_expense,
    l.due_amount,
    l.paid_amount,
    l.total_due_amount,
    l.total_paid_amount,
    l.payee,
    l.due_month_last_day,
    l.paid_month_last_day,
    -- Détermination du statut : Payé, Payé en retard, ou En attente
    CASE 
        WHEN l.paid_month_last_day IS NOT NULL AND l.paid_month_last_day = l.due_month_last_day 
            THEN 'Payé'
        WHEN l.paid_month_last_day IS NOT NULL AND l.paid_month_last_day <> l.due_month_last_day 
            THEN CONCAT('Payé en ', RIGHT(CONCAT('0', MONTH(l.paid_month_last_day)), 2),'/', YEAR(l.paid_month_last_day))
        ELSE 'En attente'
    END AS status
FROM linked_due_paid_planned_transactions l
WHERE l.row_id = 1
ORDER BY l.payee, l.due_month_last_day;


-- Vue : Historique du portefeuille de titres (quantités, PRU, valeur, performance)
CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `securities_wallet_history` AS
-- Étape 1 : Quantités cumulées par ISIN
WITH wallet_qty_securities AS (
    SELECT
        date,
        isin,
        operation_type,
        account_id,
        SUM(CASE WHEN operation_type = 'purchase' THEN quantity ELSE -quantity END) 
            OVER(PARTITION BY isin, account_id ORDER BY date) AS total_quantity
    FROM security_operations
    WHERE operation_type <> 'tax'
),

-- Étape 2 : PRU (Prix de Revient Unitaire)
wallet_price_securities AS (
    SELECT
        date,
        isin,
        SUM(quantity * net_unit_price) OVER(PARTITION BY isin ORDER BY date) AS net_amount,
        SUM(quantity * net_unit_price) OVER(PARTITION BY isin ORDER BY date)
        / SUM(quantity) OVER(PARTITION BY isin ORDER BY date) AS net_unit_price
    FROM security_operations
    WHERE operation_type = 'purchase'
),

-- Étape 3 : Valeur et rendement par titre
wallet_securities_evolution AS (
    SELECT
        sp.date,
        wqs.account_id,
        wqs.isin,
        wqs.total_quantity,
        wps.net_unit_price AS unit_cost_price,
        wqs.total_quantity * wps.net_unit_price AS cost_price,
        sp.open_price,
        sp.close_price,
        wqs.total_quantity * sp.close_price AS capital_value,
        wqs.total_quantity * sp.close_price - wqs.total_quantity * wps.net_unit_price AS capital_gain_value,
        sp.close_price / wps.net_unit_price - 1 AS capital_gain_rate,
        ROW_NUMBER() OVER(PARTITION BY wqs.isin, sp.date ORDER BY wqs.date DESC) AS row_id
    FROM wallet_qty_securities wqs
    LEFT JOIN wallet_price_securities wps ON wqs.isin = wps.isin AND wqs.date = wps.date
    LEFT JOIN wallet_qty_securities sold 
        ON wqs.isin = sold.isin AND wqs.account_id = sold.account_id AND sold.total_quantity = 0
    LEFT JOIN security_prices sp 
        ON wqs.isin = sp.isin 
        AND wqs.date <= sp.date 
        AND COALESCE(sold.date, NOW()) >= sp.date
    WHERE wqs.total_quantity > 0
),

-- Étape 4 : Rendement global pondéré par capital investi
wallet_securities_evolution_global_rate AS (
    SELECT
        date,
        SUM(cost_price * capital_gain_rate) / NULLIF(SUM(cost_price), 0) AS global_gain_rate
    FROM wallet_securities_evolution
    WHERE row_id = 1
    GROUP BY date
),

-- Étape 5 : Variation journalière du rendement global
wallet_securities_evolution_global AS (
    SELECT 
        date,
        global_gain_rate,
        COALESCE(global_gain_rate - LAG(global_gain_rate, 1) OVER (ORDER BY date), 0) AS global_gain_rate_since_yesterday
    FROM wallet_securities_evolution_global_rate
)

-- Résultat final
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

-- Vue : Historique des soldes (transactions + titres)
CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `balance_history` AS
WITH transactions AS (
    -- Regroupe les transactions mensuelles (cf. vue transactions_monthly_details)
    SELECT 
        month_last_day,
        account_id,
        is_expense,
        SUM(amount) AS amount
    FROM transactions_monthly_details
    GROUP BY month_last_day, account_id, is_expense
),
securities AS (
    -- Valeur mensuelle totale des titres détenus
    SELECT 
        DATE,
        account_id,
        SUM(capital_value) AS amount
    FROM securities_wallet_history
    GROUP BY DATE, account_id
)
-- Union : flux de trésorerie (transactions) + valorisation des titres
SELECT 
    t.month_last_day,
    t.account_id,
    SUM(CASE WHEN t.is_expense = 0 THEN t.amount ELSE -t.amount END) 
        + COALESCE(s.amount, 0) AS amount
FROM transactions t
LEFT JOIN securities s 
    ON t.month_last_day = s.date 
   AND t.account_id = s.account_id
GROUP BY t.month_last_day, t.account_id, s.amount;


-- Vue : Tableau d’amortissement des prêts
CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `amortization_table` AS
WITH RECURSIVE amortization AS (
    -- 1. Ligne initiale avant la 1ère échéance
    SELECT 
        l.id,
        0 AS due_month, 
        l.loan_start_date AS start_date,
        l.first_payment_date AS end_date,
        l.loan_amount AS initial_due_amount, -- capital initial
        0 AS capital_reimbursed_amount,
        0 AS interest_amount,
        l.interest_rate/12/100 AS interest_rate, -- taux mensuel
        0 AS monthly_payment_amount
    FROM loans l

    UNION ALL

    -- 2. Lignes suivantes (mensualités constantes méthode française)
    SELECT 
        a.id,
        a.due_month + 1,
        a.end_date,
        DATE_ADD(a.end_date, INTERVAL 1 MONTH),
        -- capital restant dû (actualisé)
        a.initial_due_amount - (a.monthly_payment_amount - a.interest_amount),
        a.monthly_payment_amount - a.interest_amount AS capital_reimbursed_amount,
        a.initial_due_amount * a.interest_rate AS interest_amount,
        a.interest_rate,
        ROUND(
            l.loan_amount * (a.interest_rate/(1-POWER(1+a.interest_rate,-l.term))),
            2
        ) AS monthly_payment_amount
    FROM amortization a
    JOIN loans l ON a.id = l.id
    WHERE a.due_month < l.term
),
reimbursements AS (
    -- Regroupe les remboursements anticipés par mois
    SELECT
        loan_id,
        LAST_DAY(date) AS reimbursement_date,
        SUM(amount) AS reimbursement_amount
    FROM loan_reimbursements
    GROUP BY loan_id, LAST_DAY(date)
)
-- Résultat final : tableau complet avec capital, intérêts, mensualités et remboursements
SELECT 
    a.id,
    a.due_month,
    a.start_date,
    a.end_date,
    a.initial_due_amount,
    a.capital_reimbursed_amount,
    a.interest_amount,
    a.monthly_payment_amount,
    r.reimbursement_amount,
    a.interest_rate,
    l.term,
    l.first_payment_date,
    l.last_payment_date
FROM amortization a
LEFT JOIN reimbursements r 
    ON a.id = r.loan_id 
   AND a.end_date = r.reimbursement_date
JOIN loans l ON a.id = l.id;


-- Vue : Synthèse globale sur 15 mois (patrimoine, dettes, revenus/dépenses, épargne)
CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `global_finance_data_15_month` AS
WITH securities AS (
    -- Valeur totale mensuelle des actifs boursiers
    SELECT 
        DATE,
        SUM(capital_value) AS securities_amount
    FROM securities_wallet_history
    GROUP BY DATE
),
transactions AS (
    -- Agrégation des transactions mensuelles (revenus / dépenses)
    SELECT 
        month_last_day,
        is_expense,
        SUM(amount) AS amount
    FROM transactions_monthly_details
    GROUP BY month_last_day, is_expense
),
assets AS (
    -- Total actifs = titres + flux financiers
    SELECT 
        t.month_last_day,
        COALESCE(SUM(CASE WHEN t.is_expense = 0 THEN t.amount ELSE -t.amount END),0)
        + COALESCE(s.securities_amount,0) AS amount
    FROM transactions t
    LEFT JOIN securities s ON t.month_last_day = s.date
    GROUP BY t.month_last_day, s.securities_amount
),
amortization_table AS (
    -- Total restant dû (on prend la dernière échéance connue pour chaque prêt)
    SELECT 
        id,
        end_date,
        initial_due_amount AS end_month_total_due_amount,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY end_date DESC) AS row_id
    FROM amortization_table
)
-- Résumé final (15 mois)
SELECT 
    a.month_last_day,
    a.amount AS gross_value, -- actifs totaux
    amt.end_month_total_due_amount AS due_amount, -- dettes
    a.amount - amt.end_month_total_due_amount AS net_value, -- valeur nette
    t.amount AS revenue, -- revenus
    exp.amount AS expenses, -- dépenses
    a.amount - amt.end_month_total_due_amount - COALESCE(t.amount,0) + COALESCE(exp.amount,0) AS savings, -- épargne
    (a.amount - amt.end_month_total_due_amount - COALESCE(t.amount,0) + COALESCE(exp.amount,0))
    / NULLIF(a.amount - amt.end_month_total_due_amount + COALESCE(exp.amount,0),0) AS saving_rate -- taux d’épargne
FROM assets a
LEFT JOIN amortization_table amt 
    ON a.month_last_day = amt.end_date AND amt.row_id = 1
LEFT JOIN transactions t 
    ON a.month_last_day = t.month_last_day AND t.is_expense = 0
LEFT JOIN transactions exp 
    ON a.month_last_day = exp.month_last_day AND exp.is_expense = 1
WHERE a.month_last_day >= DATE_FORMAT(DATE_ADD(NOW(), INTERVAL -15 MONTH), '%Y-%m-01');
