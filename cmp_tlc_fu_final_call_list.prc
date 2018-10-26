create or replace procedure cmp_tlc_fu_final_call_list is
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
      AP_PUBLIC.CORE_LOG_PKG.pInit( 'AP_CRM', 'CMP_TLC_FU_FINAL_CALL_LIST');
      execute immediate 'alter session set nls_date_language = ''AMERICAN''';
      ptruncate('cmp_tlc_fu_call_list');
      pstats('cmp_tlc_fu_call_list');
      AP_PUBLIC.CORE_LOG_PKG.pStart('Compile Final Call List');
      insert /*+ APPEND */ into cmp_tlc_fu_call_list
      select case when trunc(fcc.DTIME_APPROVAL) <> to_date('01/01/3000', 'mm/dd/yyyy') then bs.name_provider || ' - PTS'
                  else bs.name_provider || ' - PTV' end tlc_fu_call_source, 
             bs.text_contract_number contract_id, to_char(bs.id_cuid) cuid, bs.name_full tlc_fu_customer_name, bs.text_contact tlc_fu_phone_number,
             case when cl.pos_code is not null then 'Code Salesroom : ' || cl.pos_code else '' end || case when cl.pos_name is not null then ', Name Salesroom : ' || cl.pos_name else '' end tlc_fu_p_pos_info1,
             vtp.FullAddress tlc_fu_p_pos_info2,
             case when cl.promise_datetime is not null then to_char(cl.promise_datetime, 'dd-MON-yyyy hh24:mi:ss') end tlc_fu_p_date,
             bs.name_loan_purpose tlc_fu_campaign_purpose,
             bs.max_limit tlc_fu_max_credit_amount,
             ceil(((bs.default_interest_rate/100) * bs.min_limit * bs.max_tenor + bs.min_limit)/bs.max_tenor)+5000 tlc_fu_min_installment,
             bs.default_interest_rate tlc_fu_interest,
             bs.max_tenor tlc_fu_max_tenor,
             case when trunc(bs.date_application_expiry) = '01-01-3000' then 
                       to_char(trunc(fcc.DTIME_APPROVAL) + 14, 'dd-Mon-yyyy') 
                  else to_char(bs.date_application_expiry, 'dd-Mon-yyyy')
             end tlc_auto_cancel_date,
             nvl(to_char(trunc(bs.dtime_get_leads) + 30,'dd Mon yyyy'),'- Leads not found -') tlc_fu_info1,
             null tlc_fu_info2,
             null tlc_fu_info3,
             null tlc_fu_info4,
             null tlc_fu_info5,
             null tlc_fu_info6,
             null tlc_fu_info7,
             null tlc_fu_info8,
             null tlc_fu_info9,
             null tlc_fu_info10,
             'TELCO_FU' CAMPAIGN_TYPE,
             ROW_NUMBER() OVER (ORDER BY date_application_expiry ASC) NUMS,
						 trunc(sysdate)
      from ap_crm.cmp_tlc_fu_base bs
      left join ap_crm.cmp_tlc_fu_comm_list cl1 on bs.skp_credit_case = cl1.skp_credit_case
      left join owner_dwh.f_Credit_case_ad fcc on bs.skp_credit_case = fcc.SKp_CREDIT_CASE 
      left join
      (
           select cuid, contract_code, pos_code, pos_name, promise_datetime  from ap_bicc.ft_telco_appointment_tt
            where (cuid, contract_code, rec_id) in
                  (
                      select cuid, contract_code, max(rec_id)rec_id from ap_bicc.ft_telco_appointment_tt
                      where contract_code in (select text_contract_number from cmp_tlc_fu_base)
                      group by cuid, contract_code
                  )
      )cl on bs.text_contract_number = cl.contract_code and bs.id_cuid = cl.cuid
      left join ap_bicc.v_telco_pos vtp on cl.pos_code = vtp.code_salesroom
      where nvl(cl1.action, 'Call') = 'Call'
      and nvl(trunc(bs.dtime_get_leads), trunc(sysdate)) >= trunc(sysdate-30);
      AP_PUBLIC.CORE_LOG_PKG.pEnd;
      commit;
      pstats('cmp_tlc_fu_call_list');
      AP_PUBLIC.CORE_LOG_PKG.pFinish;
end;

