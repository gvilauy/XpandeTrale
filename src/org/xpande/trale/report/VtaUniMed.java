package org.xpande.trale.report;

import org.adempiere.exceptions.AdempiereException;
import org.compiere.process.ProcessInfoParameter;
import org.compiere.process.SvrProcess;
import org.compiere.util.DB;
import org.compiere.util.Env;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;

/**
 * Product: Adempiere ERP & CRM Smart Business Solution. Localization : Uruguay - Xpande
 * Xpande. Created by Gabriel Vila on 1/4/21.
 */
public class VtaUniMed extends SvrProcess {

    private final String TABLA_REPORTE = "Z_RP_VtasUniMed";

    private int adOrgID = 0;
    private int cBPartnerID = 0;
    private int mProductCategoryID = 0;
    private int cUomID = 0;
    private Timestamp startDate = null;
    private Timestamp endDate = null;


    @Override
    protected void prepare() {

        ProcessInfoParameter[] para = getParameter();

        for (int i = 0; i < para.length; i++){

            String name = para[i].getParameterName();

            if (name != null){
                if (para[i].getParameter() != null){
                    if (name.trim().equalsIgnoreCase("AD_Org_ID")){
                        this.adOrgID = ((BigDecimal)para[i].getParameter()).intValueExact();
                    }
                    else if (name.trim().equalsIgnoreCase("C_BPartner_ID")){
                        this.cBPartnerID = ((BigDecimal)para[i].getParameter()).intValueExact();
                    }
                    else if (name.trim().equalsIgnoreCase("M_Product_Category_ID")){
                        this.mProductCategoryID = ((BigDecimal)para[i].getParameter()).intValueExact();
                    }
                    else if (name.trim().equalsIgnoreCase("C_UOM_ID")){
                        this.cUomID = ((BigDecimal)para[i].getParameter()).intValueExact();
                    }
                    else if (name.trim().equalsIgnoreCase("DateTrx")){
                        this.startDate = (Timestamp)para[i].getParameter();
                        this.endDate = (Timestamp)para[i].getParameter_To();
                    }
                }
            }
        }
    }

    @Override
    protected String doIt() throws Exception {

        this.deleteData();

        this.getData();

        this.updateData();

        return "OK";
    }

    /***
     * Elimina información anterior para este usuario en tablas de reporte
     * Xpande. Created by Gabriel Vila on 1/4/21.
     */
    private void deleteData() {

        try{
            String action = " delete from " + TABLA_REPORTE + " where ad_user_id =" + this.getAD_User_ID();
            DB.executeUpdateEx(action, null);
        }
        catch (Exception e){
            throw new AdempiereException(e);
        }
    }

    /***
     * Obtiene datos iniciales para el reporte.
     * Xpande. Created by Gabriel Vila on 1/4/21.
     */
    private void getData(){

        String sql, action, whereClause;

        try{
            action = " insert into " + TABLA_REPORTE + " (ad_client_id, ad_org_id, ad_user_id, c_doctype_id, documentnoref, c_bpartner_id, "
                    + " c_invoice_id, c_invoiceline_id, datetrx, m_product_id, c_uom_id, qtyinvoiced, linenetamt, c_currency_id, "
                    + " c_uom_to_id, qtyentered ) ";

            whereClause = " and h.dateinvoiced between '" + this.startDate + "' and '" + this.endDate + "'";

            if (this.cBPartnerID > 0) whereClause += " and h.c_bpartner_id =" + this.cBPartnerID;
            if (this.mProductCategoryID > 0) whereClause += " and prod.m_product_id =" + this.mProductCategoryID;
            if (this.cUomID > 0) whereClause += " and l.c_uom_id =" + this.cUomID;

            sql = " select h.ad_client_id, h.ad_org_id, " + this.getAD_User_ID() + ", h.c_doctypetarget_id, " +
                    " h.documentno, h.c_bpartner_id, h.c_invoice_id, l.c_invoiceline_id, h.dateinvoiced, "
                    + " l.m_product_id, prod.c_uom_id, "
                    + " case when (doc.docbasetype='ARI') then l.qtyinvoiced else (l.qtyinvoiced * -1) end as qtyinvoiced, "
                    + " case when (doc.docbasetype='ARI') then l.linenetamt else (l.linenetamt * -1) end as linenetamt, "
                    + " h.c_currency_id, prod.c_uom_id, "
                    + " case when (doc.docbasetype='ARI') then l.qtyinvoiced else (l.qtyinvoiced * -1) end as qtyinvoiced2 "
                    + " from c_invoice h "
                    + " inner join c_invoiceline l on h.c_invoice_id = l.c_invoice_id "
                    + " inner join m_product prod on l.m_product_id = prod.m_product_id "
                    + " inner join c_doctype doc on h.c_doctypetarget_id = doc.c_doctype_id "
                    + " where h.ad_org_id =" + this.adOrgID
                    + " and h.docstatus='CO' "
                    + " and h.issotrx='Y' " + whereClause;

            DB.executeUpdateEx(action + sql, null);

        }
        catch (Exception e){
            throw new AdempiereException(e);
        }
    }

    /***
     * Actualiza info para el reporte.
     * Xpande. Created by Gabriel Vila on 1/4/21.
     */
    private void updateData(){

        String sql = "";
        ResultSet rs = null;
        PreparedStatement pstmt = null;

        try{

            sql = " SELECT * " +
                    " FROM " + TABLA_REPORTE +
                    " WHERE ad_user_id =?" +
                    " ORDER BY datetrx ";

            pstmt = DB.prepareStatement(sql, null);
            pstmt.setInt(1, this.getAD_User_ID());

            rs = pstmt.executeQuery ();

            String action = "";

            while (rs.next()){

                BigDecimal factor = null;
                factor = DB.getSQLValueBDEx(null, "select dividerate from c_uom_conversion "
                        + " where m_product_id =" + rs.getInt("M_Product_ID")
                        + " and c_uom_to_id =" + this.cUomID);
                if (factor != null){
                    if (factor.compareTo(Env.ZERO) > 0){
                        BigDecimal qtyEntered = rs.getBigDecimal("QtyInvoiced").multiply(factor).setScale(2, RoundingMode.HALF_UP);
                        action = " UPDATE " + TABLA_REPORTE
                                + " SET c_uom_to_id =" + this.cUomID + ", "
                                + " qtyentered =" + qtyEntered
                                + " WHERE c_invoiceline_id =" + rs.getInt("c_invoiceline_id");
                        DB.executeUpdateEx(action, null);
                    }
                }

            }

        }
        catch (Exception e)
        {
            throw new AdempiereException(e);
        }
        finally
        {
            DB.close(rs, pstmt);
            rs = null; pstmt = null;
        }

    }
}
