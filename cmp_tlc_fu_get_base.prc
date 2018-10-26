create or replace procedure CMP_TLC_FU_GET_BASE is
      PROCEDURE pStats( acTable VARCHAR2, anPerc NUMBER DEFAULT 0.01) IS
      BEGIN
          AP_PUBLIC.CORE_LOG_PKG.pStart( 'Stat:'||upper(acTable) );
          DBMS_STATS.Gather_Table_Stats( OwnName => 'AP_CRM', TabName => upper(acTable),Estimate_Percent => anPerc );
          AP_PUBLIC.CORE_LOG_PKG.pEnd;
      END;
      PROCEDURE pTruncate( acTable VARCHAR2) IS
      BEGIN
          AP_PUBLIC.CORE_LOG_PKG.pStart( 'Trunc:'||upper(acTable) );
          EXECUTE IMMEDIATE 'TRUNCATE TABLE AP_CRM.'||upper(acTable) ;
          AP_PUBLIC.CORE_LOG_PKG.pEnd ;
      END;
begin
    AP_PUBLIC.CORE_LOG_PKG.pInit( 'AP_CRM', 'CMP_TLC_FU_GET_BASE');
    ptruncate('cmp_tlc_fu_call_list');
    ptruncate('cmp_tlc_fu_comm_list');
    ptruncate('cmp_tlc_fu_base');
    AP_PUBLIC.CORE_LOG_PKG.pStart('Insert Telco FU Base');
    insert /*+ APPEND */ into cmp_tlc_fu_base
    SELECT /*+ PARALLEL(4) USE_HASH(fct fab fccl fccr fccd vtc tpr ccs clp) */ distinct fct.skp_client, fct.skp_credit_case, dcl.id_cuid, fct.text_contract_number, initcap(fccl.name_full)name_full, fccl.code_gender, fct.dtime_proposal, fct.date_application_expiry,
            coalesce(fct.amt_credit, fab.AMT_CREDIT)amt_credit, coalesce(fct.num_tenor, fab.CNT_INSTALMENT)num_tenor,
           fab.RATE_INTEREST_PRESENTED_1 * 100 rate_interest, fct.text_loan_purpose, fct.code_credit_substatus, fct.name_provider, ccs.NAME_CREDIT_STATUS,
           clp."NAME_LOAN_PURPOSE", fccr.text_contact, fccd.code_document_id iD_KTP, vtc."RISK_GROUP", vtc."MIN_LIMIT", vtc."MAX_LIMIT",
           vtc."DTIME_GET_LEADS", tpr.INTEREST_RATE default_interest_rate, TPR.MAX_TENOR
    FROM ap_bicc.FT_CREDIT_TELCO_AD fct
    left join owner_dwh.dc_client dcl on fct.skp_client = dcl.skp_client
    left join owner_Dwh.f_Application_base_tt fab on fct.skp_credit_case = fab.skp_credit_case
    left join owner_Dwh.f_Credit_case_request_clnt_tt fccl on fct.skp_credit_case = fccl.skp_credit_case and fccl.skp_client_role = 11
    left join owner_dwh.f_Credit_Case_Request_Cont_Tt fccr on fct.skp_credit_case = fccr.skp_credit_case and fccr.skp_contact_type = 2 /* Primary mobile number, change with what ever in production */
    left join owner_dwh.f_Credit_Case_Req_Clnt_Doc_Tt fccd on fct.skp_credit_case = fccd.skp_credit_case and fccd.skp_client_document_type in (9, 127, 384554) /* change with what ever in production */and fccd.flag_archived = 'N' and fccd.flag_deleted = 'N'
    left join app_bicc.v_trusting_calling_list vtc on fccd.code_document_id = vtc."TEXT_NATIONAL_ID" or trim(fccr.text_contact) = trim(substr(vtc."MSISDN",3))
    left join ap_crm.telco_product_risk_group tpr on fct.code_promo = tpr.bound_offer_name
    left join owner_dwh.cl_credit_status ccs on fct.code_credit_status = ccs.CODE_CREDIT_STATUS
    left join ap_bicc.cl_loan_purpose clp on fct.text_loan_purpose = clp."CODE_LOAN_PURPOSE"
    where ccs.SKP_CREDIT_STATUS in (1, 6, 7) /* Approved, In Preprocess, In Process */ and fct.code_credit_substatus not in ('RLF','rlf') and fct.dtime_proposal >= to_date('03/26/2018','mm/dd/yyyy')
    and vtc.RISK_GROUP not in ('D', 'E')
    ;
    AP_PUBLIC.CORE_LOG_PKG.pEnd ;
    commit;
    pStats('cmp_tlc_fu_base');
    AP_PUBLIC.CORE_LOG_PKG.pFinish;
end;
/

