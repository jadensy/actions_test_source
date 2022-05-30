select
  'Loan | CM | BV' as user_group,
  card_options,
  c
from
(select
  t1.c1 as card_options,
  count(*) as c
from
  (select
    case
      when cards like'%CreditScoreDownCard%' then'CreditScoreDownCard'
      when cards like'%YouGotPaidCard%' then'YouGotPaidCard'
      when cards like'%ATMFeeCard%' then'ATMFeeCard'
      when cards like'%CreditScoreCard%' then'CreditScoreCard'
      when cards like'%DiyTaxesBlogCard%' then'DiyTaxesBlogCard'
      when cards like'%EarnRewardsCard%' then'EarnRewardsCard'
      when cards like'%GetLoanCard%' then'GetLoanCard'
      when cards like'%LargePurchaseCard%' then'LargePurchaseCard'
      when cards like'%LinkBankAccountsCard%' then'LinkBankAccountsCard'
      when cards like'%LowBalanceCard%' then'LowBalanceCard'
      when cards like'%OverdraftFeeCard%' then'OverdraftFeeCard'
      when cards like'%PreteenFinancesBlogCard%' then'PreteenFinancesBlogCard'
      when cards like'%StudentLoanCard%' then'StudentLoanCard'
      when cards like'%TaxReturnBlogCard%' then'TaxReturnBlogCard'
      when cards like'%ExplainingStockMarketBlogCard%' then'ExplainingStockMarketBlogCard'
      when cards like'%ChoosingRetirementAccountBlogCard%' then'ChoosingRetirementAccountBlogCard'
      when cards like'%EarnedMoreCard%' then'EarnedMoreCard'
      when cards like'%CreditScoreCreditReportDiffBlogCard%' then'CreditScoreCreditReportDiffBlogCard'
      when cards like'%GetRewardedForPurchasesBlogCard%' then'GetRewardedForPurchasesBlogCard'
      when cards like'%CompoundInterestBlogCard%' then'CompoundInterestBlogCard'
      when cards like'%AreOnlineLoansSafeBlogCard%' then'AreOnlineLoansSafeBlogCard'
      when cards like'%GigEconomyBlogCard%' then'GigEconomyBlogCard'
      when cards like'%RetirementCalculatorBlogCard%' then'RetirementCalculatorBlogCard'
      when cards like'%MajorPurchasePlanningBlogCard%' then'MajorPurchasePlanningBlogCard'
      when cards like'%DebtConsolidationBlogCard%' then'DebtConsolidationBlogCard'
      when cards like'%CreditCardsTipsBlogCard%' then'CreditCardsTipsBlogCard'
      when cards like'%CarInsuranceTipsBlogCard%' then'CarInsuranceTipsBlogCard'
      when cards like'%LimitedIncomeDebtReliefBlogCard%' then'LimitedIncomeDebtReliefBlogCard'
      when cards like'%FinancialDecisionsByDecadeBlogCard%' then'FinancialDecisionsByDecadeBlogCard'
      when cards like'%StartBuildingYourSavingsBlogCard%' then'StartBuildingYourSavingsBlogCard'
      when cards like'%SummerFinancesBlogCard%' then'SummerFinancesBlogCard'
      when cards like'%WhatsFintechBlogCard%' then'WhatsFintechBlogCard'
      when cards like'%DuplicateChargeCard%' then'DuplicateChargeCard'
      when cards like'%PersonalLoansBlogCard%' then'PersonalLoansBlogCard'
      when cards like'%FreeBudgetTemplateBlogCard%' then'FreeBudgetTemplateBlogCard'
      when cards like'%HolidaySavingsBlogCard%' then'HolidaySavingsBlogCard'
      when cards like'%WeddingLoanBlogCard%' then'WeddingLoanBlogCard'
      when cards like'%RoboAdvisorsBlogCard%' then'RoboAdvisorsBlogCard'
      when cards like'%ShoppingDealsBlogCard%' then'ShoppingDealsBlogCard'
      when cards like'%HiddenFeesBlogCard%' then'HiddenFeesBlogCard'
      when cards like'%CreditScoreCriticalBlogCard%' then'CreditScoreCriticalBlogCard'
      when cards like'%BankMaintenanceFeeCard%' then'BankMaintenanceFeeCard'
      when cards like'%CreditMonitoringBenefitsBlogCard%' then'CreditMonitoringBenefitsBlogCard'
      when cards like'%InvestInAmericaCard%' then'InvestInAmericaCard'
      when cards like'%LifeInsuranceTipsBlogCard%' then'LifeInsuranceTipsBlogCard'
      when cards like'%GetOutOfDebt2017BlogCard%' then'GetOutOfDebt2017BlogCard'
      when cards like'%StudentLoanRefinancingBlogCard%' then'StudentLoanRefinancingBlogCard'
      when cards like'%RaiseYourCreditScoreBlogCard%' then'RaiseYourCreditScoreBlogCard'
      when cards like'%CMTipsCard%' then'CMTipsCard'
      when cards like'%LatePaymentsBlogCard%' then'LatePaymentsBlogCard'
      when cards like'%PayOffDebtStrategiesBlogCard%' then'PayOffDebtStrategiesBlogCard'
      when cards like'%WhatAreBondsBlogCard%' then'WhatAreBondsBlogCard'
      when cards like'%CarLoanTipsBlogCard%' then'CarLoanTipsBlogCard'
      when cards like'%HolidayRecoveryBlogCard%' then'HolidayRecoveryBlogCard'
      when cards like'%GiftCardBlogCard%' then'GiftCardBlogCard'
      when cards like'%BadCreditCreditCardsBlogCard%' then'BadCreditCreditCardsBlogCard'
      when cards like'%BadCreditLoanBlogCard%' then'BadCreditLoanBlogCard'
      when cards like'%ErrorFreeReportsCard%' then'ErrorFreeReportsCard'
      when cards like'%CreditScoreFactorsBlogCard%' then'CreditScoreFactorsBlogCard'
      when cards like'%PayOffAllCreditCardsCmTipsCard%' then'PayOffAllCreditCardsCmTipsCard'
      when cards like'%OnTimePayment3MonthsCmTipsCard%' then'OnTimePayment3MonthsCmTipsCard'
      when cards like'%ObtainCreditCardCmTipsCard%' then'ObtainCreditCardCmTipsCard'
      when cards like'%DecreaseCreditBalanceCmTipsCard%' then'DecreaseCreditBalanceCmTipsCard'
      when cards like'%ObtainPersonalLoanCmTipsCard%' then'ObtainPersonalLoanCmTipsCard'
      when cards like'%TransferCreditBalancesCmTipsCard%' then'TransferCreditBalancesCmTipsCard'
      when cards like'%WTSCoffeeCard%' then'WTSCoffeeCard'
      when cards like'%UpcomingLoanPaymentCard%' then'UpcomingLoanPaymentCard'
      when cards like'%WTSWirelessCard%' then'WTSWirelessCard'
      when cards like'%WTSTaxiCard%' then'WTSTaxiCard'
      when cards like'%WTSGasCard%' then'WTSGasCard'
      when cards like'%WTSFastFoodCard%' then'WTSFastFoodCard'
      when cards like'%ThrowbackThursdayCard%' then'ThrowbackThursdayCard'
      when cards like'%CreditScoreImprovementCard%' then'CreditScoreImprovementCard'
      when cards like'%BoostCard%' then'BoostCard'
      when cards like'%WTSFeesCard%' then'WTSFeesCard'
      when cards like'%WtsTVCard%' then'WtsTVCard'
      when cards like'%WTSRecurringPurchasesCard%' then'WTSRecurringPurchasesCard'
      when cards like'%SpentMoreCard%' then'SpentMoreCard'
      when cards like'%AskForDiscountCard%' then'AskForDiscountCard'
      when cards like'%PastDueLoanPaymentCard%' then'PastDueLoanPaymentCard'
      when cards like'%CouldScoreBeHigherCard%' then'CouldScoreBeHigherCard'
      when cards like'%IdentityGuardCard%' then'IdentityGuardCard'
      when cards like'%ESignCard%' then'ESignCard'
      when cards like'%Last3PurchasesCard%' then'Last3PurchasesCard'
      when cards like'%LoanFundsDepositedCard%' then'LoanFundsDepositedCard'
      when cards like'%StashCard%' then'StashCard'
      when cards like'%MonthlySpendingComparisonCard%' then'MonthlySpendingComparisonCard'
      when cards like'%HomeValueCard%' then'HomeValueCard'
      when cards like'%NextCreditScoreUpdateCard%' then'NextCreditScoreUpdateCard'
    end as c1
  from prod.card_status
  [without ML user_ids]
  and timestamp >= '2017-05-25'
  group by 1) as t1
join
  (select
    cards
  from prod.card_status
  [without ML user_ids]
  and timestamp >= '2017-05-25'
  and user_id in (select user_id from prod.enrollment_status where enrollment_status = 'success')
  and user_id in (select user_id from prod.bank_verification_status where (bank_verification_status = 'success' or bank_verification_status = 'linked'))
  and user_id in (select user_id from prod.loan_status)
  ) as t2 on
t2.cards like '%' || t1.c1 || '%'
group by 1)

union all

select
  '* | CM | *' as user_group,
  card_options,
  c
from
(select
  t1.c1 as card_options,
  count(*) as c
from
  (select
    case
      when cards like'%CreditScoreDownCard%' then'CreditScoreDownCard'
      when cards like'%YouGotPaidCard%' then'YouGotPaidCard'
      when cards like'%ATMFeeCard%' then'ATMFeeCard'
      when cards like'%CreditScoreCard%' then'CreditScoreCard'
      when cards like'%DiyTaxesBlogCard%' then'DiyTaxesBlogCard'
      when cards like'%EarnRewardsCard%' then'EarnRewardsCard'
      when cards like'%GetLoanCard%' then'GetLoanCard'
      when cards like'%LargePurchaseCard%' then'LargePurchaseCard'
      when cards like'%LinkBankAccountsCard%' then'LinkBankAccountsCard'
      when cards like'%LowBalanceCard%' then'LowBalanceCard'
      when cards like'%OverdraftFeeCard%' then'OverdraftFeeCard'
      when cards like'%PreteenFinancesBlogCard%' then'PreteenFinancesBlogCard'
      when cards like'%StudentLoanCard%' then'StudentLoanCard'
      when cards like'%TaxReturnBlogCard%' then'TaxReturnBlogCard'
      when cards like'%ExplainingStockMarketBlogCard%' then'ExplainingStockMarketBlogCard'
      when cards like'%ChoosingRetirementAccountBlogCard%' then'ChoosingRetirementAccountBlogCard'
      when cards like'%EarnedMoreCard%' then'EarnedMoreCard'
      when cards like'%CreditScoreCreditReportDiffBlogCard%' then'CreditScoreCreditReportDiffBlogCard'
      when cards like'%GetRewardedForPurchasesBlogCard%' then'GetRewardedForPurchasesBlogCard'
      when cards like'%CompoundInterestBlogCard%' then'CompoundInterestBlogCard'
      when cards like'%AreOnlineLoansSafeBlogCard%' then'AreOnlineLoansSafeBlogCard'
      when cards like'%GigEconomyBlogCard%' then'GigEconomyBlogCard'
      when cards like'%RetirementCalculatorBlogCard%' then'RetirementCalculatorBlogCard'
      when cards like'%MajorPurchasePlanningBlogCard%' then'MajorPurchasePlanningBlogCard'
      when cards like'%DebtConsolidationBlogCard%' then'DebtConsolidationBlogCard'
      when cards like'%CreditCardsTipsBlogCard%' then'CreditCardsTipsBlogCard'
      when cards like'%CarInsuranceTipsBlogCard%' then'CarInsuranceTipsBlogCard'
      when cards like'%LimitedIncomeDebtReliefBlogCard%' then'LimitedIncomeDebtReliefBlogCard'
      when cards like'%FinancialDecisionsByDecadeBlogCard%' then'FinancialDecisionsByDecadeBlogCard'
      when cards like'%StartBuildingYourSavingsBlogCard%' then'StartBuildingYourSavingsBlogCard'
      when cards like'%SummerFinancesBlogCard%' then'SummerFinancesBlogCard'
      when cards like'%WhatsFintechBlogCard%' then'WhatsFintechBlogCard'
      when cards like'%DuplicateChargeCard%' then'DuplicateChargeCard'
      when cards like'%PersonalLoansBlogCard%' then'PersonalLoansBlogCard'
      when cards like'%FreeBudgetTemplateBlogCard%' then'FreeBudgetTemplateBlogCard'
      when cards like'%HolidaySavingsBlogCard%' then'HolidaySavingsBlogCard'
      when cards like'%WeddingLoanBlogCard%' then'WeddingLoanBlogCard'
      when cards like'%RoboAdvisorsBlogCard%' then'RoboAdvisorsBlogCard'
      when cards like'%ShoppingDealsBlogCard%' then'ShoppingDealsBlogCard'
      when cards like'%HiddenFeesBlogCard%' then'HiddenFeesBlogCard'
      when cards like'%CreditScoreCriticalBlogCard%' then'CreditScoreCriticalBlogCard'
      when cards like'%BankMaintenanceFeeCard%' then'BankMaintenanceFeeCard'
      when cards like'%CreditMonitoringBenefitsBlogCard%' then'CreditMonitoringBenefitsBlogCard'
      when cards like'%InvestInAmericaCard%' then'InvestInAmericaCard'
      when cards like'%LifeInsuranceTipsBlogCard%' then'LifeInsuranceTipsBlogCard'
      when cards like'%GetOutOfDebt2017BlogCard%' then'GetOutOfDebt2017BlogCard'
      when cards like'%StudentLoanRefinancingBlogCard%' then'StudentLoanRefinancingBlogCard'
      when cards like'%RaiseYourCreditScoreBlogCard%' then'RaiseYourCreditScoreBlogCard'
      when cards like'%CMTipsCard%' then'CMTipsCard'
      when cards like'%LatePaymentsBlogCard%' then'LatePaymentsBlogCard'
      when cards like'%PayOffDebtStrategiesBlogCard%' then'PayOffDebtStrategiesBlogCard'
      when cards like'%WhatAreBondsBlogCard%' then'WhatAreBondsBlogCard'
      when cards like'%CarLoanTipsBlogCard%' then'CarLoanTipsBlogCard'
      when cards like'%HolidayRecoveryBlogCard%' then'HolidayRecoveryBlogCard'
      when cards like'%GiftCardBlogCard%' then'GiftCardBlogCard'
      when cards like'%BadCreditCreditCardsBlogCard%' then'BadCreditCreditCardsBlogCard'
      when cards like'%BadCreditLoanBlogCard%' then'BadCreditLoanBlogCard'
      when cards like'%ErrorFreeReportsCard%' then'ErrorFreeReportsCard'
      when cards like'%CreditScoreFactorsBlogCard%' then'CreditScoreFactorsBlogCard'
      when cards like'%PayOffAllCreditCardsCmTipsCard%' then'PayOffAllCreditCardsCmTipsCard'
      when cards like'%OnTimePayment3MonthsCmTipsCard%' then'OnTimePayment3MonthsCmTipsCard'
      when cards like'%ObtainCreditCardCmTipsCard%' then'ObtainCreditCardCmTipsCard'
      when cards like'%DecreaseCreditBalanceCmTipsCard%' then'DecreaseCreditBalanceCmTipsCard'
      when cards like'%ObtainPersonalLoanCmTipsCard%' then'ObtainPersonalLoanCmTipsCard'
      when cards like'%TransferCreditBalancesCmTipsCard%' then'TransferCreditBalancesCmTipsCard'
      when cards like'%WTSCoffeeCard%' then'WTSCoffeeCard'
      when cards like'%UpcomingLoanPaymentCard%' then'UpcomingLoanPaymentCard'
      when cards like'%WTSWirelessCard%' then'WTSWirelessCard'
      when cards like'%WTSTaxiCard%' then'WTSTaxiCard'
      when cards like'%WTSGasCard%' then'WTSGasCard'
      when cards like'%WTSFastFoodCard%' then'WTSFastFoodCard'
      when cards like'%ThrowbackThursdayCard%' then'ThrowbackThursdayCard'
      when cards like'%CreditScoreImprovementCard%' then'CreditScoreImprovementCard'
      when cards like'%BoostCard%' then'BoostCard'
      when cards like'%WTSFeesCard%' then'WTSFeesCard'
      when cards like'%WtsTVCard%' then'WtsTVCard'
      when cards like'%WTSRecurringPurchasesCard%' then'WTSRecurringPurchasesCard'
      when cards like'%SpentMoreCard%' then'SpentMoreCard'
      when cards like'%AskForDiscountCard%' then'AskForDiscountCard'
      when cards like'%PastDueLoanPaymentCard%' then'PastDueLoanPaymentCard'
      when cards like'%CouldScoreBeHigherCard%' then'CouldScoreBeHigherCard'
      when cards like'%IdentityGuardCard%' then'IdentityGuardCard'
      when cards like'%ESignCard%' then'ESignCard'
      when cards like'%Last3PurchasesCard%' then'Last3PurchasesCard'
      when cards like'%LoanFundsDepositedCard%' then'LoanFundsDepositedCard'
      when cards like'%StashCard%' then'StashCard'
      when cards like'%MonthlySpendingComparisonCard%' then'MonthlySpendingComparisonCard'
      when cards like'%HomeValueCard%' then'HomeValueCard'
      when cards like'%NextCreditScoreUpdateCard%' then'NextCreditScoreUpdateCard'
    end as c1
  from prod.card_status
  [without ML user_ids]
  and timestamp >= '2017-05-25'
  group by 1) as t1
join
  (select
    cards
  from prod.card_status
  [without ML user_ids]
  and timestamp >= '2017-05-25'
  and user_id in (select user_id from prod.enrollment_status where enrollment_status = 'success')
  ) as t2 on
t2.cards like '%' || t1.c1 || '%'
group by 1)

union all

select
  'Loan | * | *' as user_group,
  card_options,
  c
from
(select
  t1.c1 as card_options,
  count(*) as c
from
  (select
    case
      when cards like'%CreditScoreDownCard%' then'CreditScoreDownCard'
      when cards like'%YouGotPaidCard%' then'YouGotPaidCard'
      when cards like'%ATMFeeCard%' then'ATMFeeCard'
      when cards like'%CreditScoreCard%' then'CreditScoreCard'
      when cards like'%DiyTaxesBlogCard%' then'DiyTaxesBlogCard'
      when cards like'%EarnRewardsCard%' then'EarnRewardsCard'
      when cards like'%GetLoanCard%' then'GetLoanCard'
      when cards like'%LargePurchaseCard%' then'LargePurchaseCard'
      when cards like'%LinkBankAccountsCard%' then'LinkBankAccountsCard'
      when cards like'%LowBalanceCard%' then'LowBalanceCard'
      when cards like'%OverdraftFeeCard%' then'OverdraftFeeCard'
      when cards like'%PreteenFinancesBlogCard%' then'PreteenFinancesBlogCard'
      when cards like'%StudentLoanCard%' then'StudentLoanCard'
      when cards like'%TaxReturnBlogCard%' then'TaxReturnBlogCard'
      when cards like'%ExplainingStockMarketBlogCard%' then'ExplainingStockMarketBlogCard'
      when cards like'%ChoosingRetirementAccountBlogCard%' then'ChoosingRetirementAccountBlogCard'
      when cards like'%EarnedMoreCard%' then'EarnedMoreCard'
      when cards like'%CreditScoreCreditReportDiffBlogCard%' then'CreditScoreCreditReportDiffBlogCard'
      when cards like'%GetRewardedForPurchasesBlogCard%' then'GetRewardedForPurchasesBlogCard'
      when cards like'%CompoundInterestBlogCard%' then'CompoundInterestBlogCard'
      when cards like'%AreOnlineLoansSafeBlogCard%' then'AreOnlineLoansSafeBlogCard'
      when cards like'%GigEconomyBlogCard%' then'GigEconomyBlogCard'
      when cards like'%RetirementCalculatorBlogCard%' then'RetirementCalculatorBlogCard'
      when cards like'%MajorPurchasePlanningBlogCard%' then'MajorPurchasePlanningBlogCard'
      when cards like'%DebtConsolidationBlogCard%' then'DebtConsolidationBlogCard'
      when cards like'%CreditCardsTipsBlogCard%' then'CreditCardsTipsBlogCard'
      when cards like'%CarInsuranceTipsBlogCard%' then'CarInsuranceTipsBlogCard'
      when cards like'%LimitedIncomeDebtReliefBlogCard%' then'LimitedIncomeDebtReliefBlogCard'
      when cards like'%FinancialDecisionsByDecadeBlogCard%' then'FinancialDecisionsByDecadeBlogCard'
      when cards like'%StartBuildingYourSavingsBlogCard%' then'StartBuildingYourSavingsBlogCard'
      when cards like'%SummerFinancesBlogCard%' then'SummerFinancesBlogCard'
      when cards like'%WhatsFintechBlogCard%' then'WhatsFintechBlogCard'
      when cards like'%DuplicateChargeCard%' then'DuplicateChargeCard'
      when cards like'%PersonalLoansBlogCard%' then'PersonalLoansBlogCard'
      when cards like'%FreeBudgetTemplateBlogCard%' then'FreeBudgetTemplateBlogCard'
      when cards like'%HolidaySavingsBlogCard%' then'HolidaySavingsBlogCard'
      when cards like'%WeddingLoanBlogCard%' then'WeddingLoanBlogCard'
      when cards like'%RoboAdvisorsBlogCard%' then'RoboAdvisorsBlogCard'
      when cards like'%ShoppingDealsBlogCard%' then'ShoppingDealsBlogCard'
      when cards like'%HiddenFeesBlogCard%' then'HiddenFeesBlogCard'
      when cards like'%CreditScoreCriticalBlogCard%' then'CreditScoreCriticalBlogCard'
      when cards like'%BankMaintenanceFeeCard%' then'BankMaintenanceFeeCard'
      when cards like'%CreditMonitoringBenefitsBlogCard%' then'CreditMonitoringBenefitsBlogCard'
      when cards like'%InvestInAmericaCard%' then'InvestInAmericaCard'
      when cards like'%LifeInsuranceTipsBlogCard%' then'LifeInsuranceTipsBlogCard'
      when cards like'%GetOutOfDebt2017BlogCard%' then'GetOutOfDebt2017BlogCard'
      when cards like'%StudentLoanRefinancingBlogCard%' then'StudentLoanRefinancingBlogCard'
      when cards like'%RaiseYourCreditScoreBlogCard%' then'RaiseYourCreditScoreBlogCard'
      when cards like'%CMTipsCard%' then'CMTipsCard'
      when cards like'%LatePaymentsBlogCard%' then'LatePaymentsBlogCard'
      when cards like'%PayOffDebtStrategiesBlogCard%' then'PayOffDebtStrategiesBlogCard'
      when cards like'%WhatAreBondsBlogCard%' then'WhatAreBondsBlogCard'
      when cards like'%CarLoanTipsBlogCard%' then'CarLoanTipsBlogCard'
      when cards like'%HolidayRecoveryBlogCard%' then'HolidayRecoveryBlogCard'
      when cards like'%GiftCardBlogCard%' then'GiftCardBlogCard'
      when cards like'%BadCreditCreditCardsBlogCard%' then'BadCreditCreditCardsBlogCard'
      when cards like'%BadCreditLoanBlogCard%' then'BadCreditLoanBlogCard'
      when cards like'%ErrorFreeReportsCard%' then'ErrorFreeReportsCard'
      when cards like'%CreditScoreFactorsBlogCard%' then'CreditScoreFactorsBlogCard'
      when cards like'%PayOffAllCreditCardsCmTipsCard%' then'PayOffAllCreditCardsCmTipsCard'
      when cards like'%OnTimePayment3MonthsCmTipsCard%' then'OnTimePayment3MonthsCmTipsCard'
      when cards like'%ObtainCreditCardCmTipsCard%' then'ObtainCreditCardCmTipsCard'
      when cards like'%DecreaseCreditBalanceCmTipsCard%' then'DecreaseCreditBalanceCmTipsCard'
      when cards like'%ObtainPersonalLoanCmTipsCard%' then'ObtainPersonalLoanCmTipsCard'
      when cards like'%TransferCreditBalancesCmTipsCard%' then'TransferCreditBalancesCmTipsCard'
      when cards like'%WTSCoffeeCard%' then'WTSCoffeeCard'
      when cards like'%UpcomingLoanPaymentCard%' then'UpcomingLoanPaymentCard'
      when cards like'%WTSWirelessCard%' then'WTSWirelessCard'
      when cards like'%WTSTaxiCard%' then'WTSTaxiCard'
      when cards like'%WTSGasCard%' then'WTSGasCard'
      when cards like'%WTSFastFoodCard%' then'WTSFastFoodCard'
      when cards like'%ThrowbackThursdayCard%' then'ThrowbackThursdayCard'
      when cards like'%CreditScoreImprovementCard%' then'CreditScoreImprovementCard'
      when cards like'%BoostCard%' then'BoostCard'
      when cards like'%WTSFeesCard%' then'WTSFeesCard'
      when cards like'%WtsTVCard%' then'WtsTVCard'
      when cards like'%WTSRecurringPurchasesCard%' then'WTSRecurringPurchasesCard'
      when cards like'%SpentMoreCard%' then'SpentMoreCard'
      when cards like'%AskForDiscountCard%' then'AskForDiscountCard'
      when cards like'%PastDueLoanPaymentCard%' then'PastDueLoanPaymentCard'
      when cards like'%CouldScoreBeHigherCard%' then'CouldScoreBeHigherCard'
      when cards like'%IdentityGuardCard%' then'IdentityGuardCard'
      when cards like'%ESignCard%' then'ESignCard'
      when cards like'%Last3PurchasesCard%' then'Last3PurchasesCard'
      when cards like'%LoanFundsDepositedCard%' then'LoanFundsDepositedCard'
      when cards like'%StashCard%' then'StashCard'
      when cards like'%MonthlySpendingComparisonCard%' then'MonthlySpendingComparisonCard'
      when cards like'%HomeValueCard%' then'HomeValueCard'
      when cards like'%NextCreditScoreUpdateCard%' then'NextCreditScoreUpdateCard'
    end as c1
  from prod.card_status
  [without ML user_ids]
  and timestamp >= '2017-05-25'
  group by 1) as t1
join
  (select
    cards
  from prod.card_status
  [without ML user_ids]
  and timestamp >= '2017-05-25'
  and user_id in (select user_id from prod.loan_status)
  ) as t2 on
t2.cards like '%' || t1.c1 || '%'
group by 1)

union all

select
  '* | * | BV' as user_group,
  card_options,
  c
from
(select
  t1.c1 as card_options,
  count(*) as c
from
  (select
    case
      when cards like'%CreditScoreDownCard%' then'CreditScoreDownCard'
      when cards like'%YouGotPaidCard%' then'YouGotPaidCard'
      when cards like'%ATMFeeCard%' then'ATMFeeCard'
      when cards like'%CreditScoreCard%' then'CreditScoreCard'
      when cards like'%DiyTaxesBlogCard%' then'DiyTaxesBlogCard'
      when cards like'%EarnRewardsCard%' then'EarnRewardsCard'
      when cards like'%GetLoanCard%' then'GetLoanCard'
      when cards like'%LargePurchaseCard%' then'LargePurchaseCard'
      when cards like'%LinkBankAccountsCard%' then'LinkBankAccountsCard'
      when cards like'%LowBalanceCard%' then'LowBalanceCard'
      when cards like'%OverdraftFeeCard%' then'OverdraftFeeCard'
      when cards like'%PreteenFinancesBlogCard%' then'PreteenFinancesBlogCard'
      when cards like'%StudentLoanCard%' then'StudentLoanCard'
      when cards like'%TaxReturnBlogCard%' then'TaxReturnBlogCard'
      when cards like'%ExplainingStockMarketBlogCard%' then'ExplainingStockMarketBlogCard'
      when cards like'%ChoosingRetirementAccountBlogCard%' then'ChoosingRetirementAccountBlogCard'
      when cards like'%EarnedMoreCard%' then'EarnedMoreCard'
      when cards like'%CreditScoreCreditReportDiffBlogCard%' then'CreditScoreCreditReportDiffBlogCard'
      when cards like'%GetRewardedForPurchasesBlogCard%' then'GetRewardedForPurchasesBlogCard'
      when cards like'%CompoundInterestBlogCard%' then'CompoundInterestBlogCard'
      when cards like'%AreOnlineLoansSafeBlogCard%' then'AreOnlineLoansSafeBlogCard'
      when cards like'%GigEconomyBlogCard%' then'GigEconomyBlogCard'
      when cards like'%RetirementCalculatorBlogCard%' then'RetirementCalculatorBlogCard'
      when cards like'%MajorPurchasePlanningBlogCard%' then'MajorPurchasePlanningBlogCard'
      when cards like'%DebtConsolidationBlogCard%' then'DebtConsolidationBlogCard'
      when cards like'%CreditCardsTipsBlogCard%' then'CreditCardsTipsBlogCard'
      when cards like'%CarInsuranceTipsBlogCard%' then'CarInsuranceTipsBlogCard'
      when cards like'%LimitedIncomeDebtReliefBlogCard%' then'LimitedIncomeDebtReliefBlogCard'
      when cards like'%FinancialDecisionsByDecadeBlogCard%' then'FinancialDecisionsByDecadeBlogCard'
      when cards like'%StartBuildingYourSavingsBlogCard%' then'StartBuildingYourSavingsBlogCard'
      when cards like'%SummerFinancesBlogCard%' then'SummerFinancesBlogCard'
      when cards like'%WhatsFintechBlogCard%' then'WhatsFintechBlogCard'
      when cards like'%DuplicateChargeCard%' then'DuplicateChargeCard'
      when cards like'%PersonalLoansBlogCard%' then'PersonalLoansBlogCard'
      when cards like'%FreeBudgetTemplateBlogCard%' then'FreeBudgetTemplateBlogCard'
      when cards like'%HolidaySavingsBlogCard%' then'HolidaySavingsBlogCard'
      when cards like'%WeddingLoanBlogCard%' then'WeddingLoanBlogCard'
      when cards like'%RoboAdvisorsBlogCard%' then'RoboAdvisorsBlogCard'
      when cards like'%ShoppingDealsBlogCard%' then'ShoppingDealsBlogCard'
      when cards like'%HiddenFeesBlogCard%' then'HiddenFeesBlogCard'
      when cards like'%CreditScoreCriticalBlogCard%' then'CreditScoreCriticalBlogCard'
      when cards like'%BankMaintenanceFeeCard%' then'BankMaintenanceFeeCard'
      when cards like'%CreditMonitoringBenefitsBlogCard%' then'CreditMonitoringBenefitsBlogCard'
      when cards like'%InvestInAmericaCard%' then'InvestInAmericaCard'
      when cards like'%LifeInsuranceTipsBlogCard%' then'LifeInsuranceTipsBlogCard'
      when cards like'%GetOutOfDebt2017BlogCard%' then'GetOutOfDebt2017BlogCard'
      when cards like'%StudentLoanRefinancingBlogCard%' then'StudentLoanRefinancingBlogCard'
      when cards like'%RaiseYourCreditScoreBlogCard%' then'RaiseYourCreditScoreBlogCard'
      when cards like'%CMTipsCard%' then'CMTipsCard'
      when cards like'%LatePaymentsBlogCard%' then'LatePaymentsBlogCard'
      when cards like'%PayOffDebtStrategiesBlogCard%' then'PayOffDebtStrategiesBlogCard'
      when cards like'%WhatAreBondsBlogCard%' then'WhatAreBondsBlogCard'
      when cards like'%CarLoanTipsBlogCard%' then'CarLoanTipsBlogCard'
      when cards like'%HolidayRecoveryBlogCard%' then'HolidayRecoveryBlogCard'
      when cards like'%GiftCardBlogCard%' then'GiftCardBlogCard'
      when cards like'%BadCreditCreditCardsBlogCard%' then'BadCreditCreditCardsBlogCard'
      when cards like'%BadCreditLoanBlogCard%' then'BadCreditLoanBlogCard'
      when cards like'%ErrorFreeReportsCard%' then'ErrorFreeReportsCard'
      when cards like'%CreditScoreFactorsBlogCard%' then'CreditScoreFactorsBlogCard'
      when cards like'%PayOffAllCreditCardsCmTipsCard%' then'PayOffAllCreditCardsCmTipsCard'
      when cards like'%OnTimePayment3MonthsCmTipsCard%' then'OnTimePayment3MonthsCmTipsCard'
      when cards like'%ObtainCreditCardCmTipsCard%' then'ObtainCreditCardCmTipsCard'
      when cards like'%DecreaseCreditBalanceCmTipsCard%' then'DecreaseCreditBalanceCmTipsCard'
      when cards like'%ObtainPersonalLoanCmTipsCard%' then'ObtainPersonalLoanCmTipsCard'
      when cards like'%TransferCreditBalancesCmTipsCard%' then'TransferCreditBalancesCmTipsCard'
      when cards like'%WTSCoffeeCard%' then'WTSCoffeeCard'
      when cards like'%UpcomingLoanPaymentCard%' then'UpcomingLoanPaymentCard'
      when cards like'%WTSWirelessCard%' then'WTSWirelessCard'
      when cards like'%WTSTaxiCard%' then'WTSTaxiCard'
      when cards like'%WTSGasCard%' then'WTSGasCard'
      when cards like'%WTSFastFoodCard%' then'WTSFastFoodCard'
      when cards like'%ThrowbackThursdayCard%' then'ThrowbackThursdayCard'
      when cards like'%CreditScoreImprovementCard%' then'CreditScoreImprovementCard'
      when cards like'%BoostCard%' then'BoostCard'
      when cards like'%WTSFeesCard%' then'WTSFeesCard'
      when cards like'%WtsTVCard%' then'WtsTVCard'
      when cards like'%WTSRecurringPurchasesCard%' then'WTSRecurringPurchasesCard'
      when cards like'%SpentMoreCard%' then'SpentMoreCard'
      when cards like'%AskForDiscountCard%' then'AskForDiscountCard'
      when cards like'%PastDueLoanPaymentCard%' then'PastDueLoanPaymentCard'
      when cards like'%CouldScoreBeHigherCard%' then'CouldScoreBeHigherCard'
      when cards like'%IdentityGuardCard%' then'IdentityGuardCard'
      when cards like'%ESignCard%' then'ESignCard'
      when cards like'%Last3PurchasesCard%' then'Last3PurchasesCard'
      when cards like'%LoanFundsDepositedCard%' then'LoanFundsDepositedCard'
      when cards like'%StashCard%' then'StashCard'
      when cards like'%MonthlySpendingComparisonCard%' then'MonthlySpendingComparisonCard'
      when cards like'%HomeValueCard%' then'HomeValueCard'
      when cards like'%NextCreditScoreUpdateCard%' then'NextCreditScoreUpdateCard'
    end as c1
  from prod.card_status
  [without ML user_ids]
  and timestamp >= '2017-05-25'
  group by 1) as t1
join
  (select
    cards
  from prod.card_status
  [without ML user_ids]
  and timestamp >= '2017-05-25'
  and user_id in (select user_id from prod.bank_verification_status where (bank_verification_status = 'success' or bank_verification_status = 'linked'))
  ) as t2 on
t2.cards like '%' || t1.c1 || '%'
group by 1)

union all

select
  'New Loan | * | *' as user_group,
  card_options,
  c
from
(select
  t1.c1 as card_options,
  count(*) as c
from
  (select
    case
      when cards like'%CreditScoreDownCard%' then'CreditScoreDownCard'
      when cards like'%YouGotPaidCard%' then'YouGotPaidCard'
      when cards like'%ATMFeeCard%' then'ATMFeeCard'
      when cards like'%CreditScoreCard%' then'CreditScoreCard'
      when cards like'%DiyTaxesBlogCard%' then'DiyTaxesBlogCard'
      when cards like'%EarnRewardsCard%' then'EarnRewardsCard'
      when cards like'%GetLoanCard%' then'GetLoanCard'
      when cards like'%LargePurchaseCard%' then'LargePurchaseCard'
      when cards like'%LinkBankAccountsCard%' then'LinkBankAccountsCard'
      when cards like'%LowBalanceCard%' then'LowBalanceCard'
      when cards like'%OverdraftFeeCard%' then'OverdraftFeeCard'
      when cards like'%PreteenFinancesBlogCard%' then'PreteenFinancesBlogCard'
      when cards like'%StudentLoanCard%' then'StudentLoanCard'
      when cards like'%TaxReturnBlogCard%' then'TaxReturnBlogCard'
      when cards like'%ExplainingStockMarketBlogCard%' then'ExplainingStockMarketBlogCard'
      when cards like'%ChoosingRetirementAccountBlogCard%' then'ChoosingRetirementAccountBlogCard'
      when cards like'%EarnedMoreCard%' then'EarnedMoreCard'
      when cards like'%CreditScoreCreditReportDiffBlogCard%' then'CreditScoreCreditReportDiffBlogCard'
      when cards like'%GetRewardedForPurchasesBlogCard%' then'GetRewardedForPurchasesBlogCard'
      when cards like'%CompoundInterestBlogCard%' then'CompoundInterestBlogCard'
      when cards like'%AreOnlineLoansSafeBlogCard%' then'AreOnlineLoansSafeBlogCard'
      when cards like'%GigEconomyBlogCard%' then'GigEconomyBlogCard'
      when cards like'%RetirementCalculatorBlogCard%' then'RetirementCalculatorBlogCard'
      when cards like'%MajorPurchasePlanningBlogCard%' then'MajorPurchasePlanningBlogCard'
      when cards like'%DebtConsolidationBlogCard%' then'DebtConsolidationBlogCard'
      when cards like'%CreditCardsTipsBlogCard%' then'CreditCardsTipsBlogCard'
      when cards like'%CarInsuranceTipsBlogCard%' then'CarInsuranceTipsBlogCard'
      when cards like'%LimitedIncomeDebtReliefBlogCard%' then'LimitedIncomeDebtReliefBlogCard'
      when cards like'%FinancialDecisionsByDecadeBlogCard%' then'FinancialDecisionsByDecadeBlogCard'
      when cards like'%StartBuildingYourSavingsBlogCard%' then'StartBuildingYourSavingsBlogCard'
      when cards like'%SummerFinancesBlogCard%' then'SummerFinancesBlogCard'
      when cards like'%WhatsFintechBlogCard%' then'WhatsFintechBlogCard'
      when cards like'%DuplicateChargeCard%' then'DuplicateChargeCard'
      when cards like'%PersonalLoansBlogCard%' then'PersonalLoansBlogCard'
      when cards like'%FreeBudgetTemplateBlogCard%' then'FreeBudgetTemplateBlogCard'
      when cards like'%HolidaySavingsBlogCard%' then'HolidaySavingsBlogCard'
      when cards like'%WeddingLoanBlogCard%' then'WeddingLoanBlogCard'
      when cards like'%RoboAdvisorsBlogCard%' then'RoboAdvisorsBlogCard'
      when cards like'%ShoppingDealsBlogCard%' then'ShoppingDealsBlogCard'
      when cards like'%HiddenFeesBlogCard%' then'HiddenFeesBlogCard'
      when cards like'%CreditScoreCriticalBlogCard%' then'CreditScoreCriticalBlogCard'
      when cards like'%BankMaintenanceFeeCard%' then'BankMaintenanceFeeCard'
      when cards like'%CreditMonitoringBenefitsBlogCard%' then'CreditMonitoringBenefitsBlogCard'
      when cards like'%InvestInAmericaCard%' then'InvestInAmericaCard'
      when cards like'%LifeInsuranceTipsBlogCard%' then'LifeInsuranceTipsBlogCard'
      when cards like'%GetOutOfDebt2017BlogCard%' then'GetOutOfDebt2017BlogCard'
      when cards like'%StudentLoanRefinancingBlogCard%' then'StudentLoanRefinancingBlogCard'
      when cards like'%RaiseYourCreditScoreBlogCard%' then'RaiseYourCreditScoreBlogCard'
      when cards like'%CMTipsCard%' then'CMTipsCard'
      when cards like'%LatePaymentsBlogCard%' then'LatePaymentsBlogCard'
      when cards like'%PayOffDebtStrategiesBlogCard%' then'PayOffDebtStrategiesBlogCard'
      when cards like'%WhatAreBondsBlogCard%' then'WhatAreBondsBlogCard'
      when cards like'%CarLoanTipsBlogCard%' then'CarLoanTipsBlogCard'
      when cards like'%HolidayRecoveryBlogCard%' then'HolidayRecoveryBlogCard'
      when cards like'%GiftCardBlogCard%' then'GiftCardBlogCard'
      when cards like'%BadCreditCreditCardsBlogCard%' then'BadCreditCreditCardsBlogCard'
      when cards like'%BadCreditLoanBlogCard%' then'BadCreditLoanBlogCard'
      when cards like'%ErrorFreeReportsCard%' then'ErrorFreeReportsCard'
      when cards like'%CreditScoreFactorsBlogCard%' then'CreditScoreFactorsBlogCard'
      when cards like'%PayOffAllCreditCardsCmTipsCard%' then'PayOffAllCreditCardsCmTipsCard'
      when cards like'%OnTimePayment3MonthsCmTipsCard%' then'OnTimePayment3MonthsCmTipsCard'
      when cards like'%ObtainCreditCardCmTipsCard%' then'ObtainCreditCardCmTipsCard'
      when cards like'%DecreaseCreditBalanceCmTipsCard%' then'DecreaseCreditBalanceCmTipsCard'
      when cards like'%ObtainPersonalLoanCmTipsCard%' then'ObtainPersonalLoanCmTipsCard'
      when cards like'%TransferCreditBalancesCmTipsCard%' then'TransferCreditBalancesCmTipsCard'
      when cards like'%WTSCoffeeCard%' then'WTSCoffeeCard'
      when cards like'%UpcomingLoanPaymentCard%' then'UpcomingLoanPaymentCard'
      when cards like'%WTSWirelessCard%' then'WTSWirelessCard'
      when cards like'%WTSTaxiCard%' then'WTSTaxiCard'
      when cards like'%WTSGasCard%' then'WTSGasCard'
      when cards like'%WTSFastFoodCard%' then'WTSFastFoodCard'
      when cards like'%ThrowbackThursdayCard%' then'ThrowbackThursdayCard'
      when cards like'%CreditScoreImprovementCard%' then'CreditScoreImprovementCard'
      when cards like'%BoostCard%' then'BoostCard'
      when cards like'%WTSFeesCard%' then'WTSFeesCard'
      when cards like'%WtsTVCard%' then'WtsTVCard'
      when cards like'%WTSRecurringPurchasesCard%' then'WTSRecurringPurchasesCard'
      when cards like'%SpentMoreCard%' then'SpentMoreCard'
      when cards like'%AskForDiscountCard%' then'AskForDiscountCard'
      when cards like'%PastDueLoanPaymentCard%' then'PastDueLoanPaymentCard'
      when cards like'%CouldScoreBeHigherCard%' then'CouldScoreBeHigherCard'
      when cards like'%IdentityGuardCard%' then'IdentityGuardCard'
      when cards like'%ESignCard%' then'ESignCard'
      when cards like'%Last3PurchasesCard%' then'Last3PurchasesCard'
      when cards like'%LoanFundsDepositedCard%' then'LoanFundsDepositedCard'
      when cards like'%StashCard%' then'StashCard'
      when cards like'%MonthlySpendingComparisonCard%' then'MonthlySpendingComparisonCard'
      when cards like'%HomeValueCard%' then'HomeValueCard'
      when cards like'%NextCreditScoreUpdateCard%' then'NextCreditScoreUpdateCard'
    end as c1
  from prod.card_status
  [without ML user_ids]
  and timestamp >= '2017-05-25'
  group by 1) as t1
join
  (select
    cards
  from prod.card_status
  [without ML user_ids]
  and timestamp >= '2017-05-25'
  and user_id in (select user_id from prod.loan_status where loan_status = 'New Loan')
  ) as t2 on
t2.cards like '%' || t1.c1 || '%'
group by 1)