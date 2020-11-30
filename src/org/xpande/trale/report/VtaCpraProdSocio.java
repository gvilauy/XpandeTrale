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
 * Reporte: Ventas y Compras por Producto y Socio de Negocio. Especial para Trale.
 * Product: Adempiere ERP & CRM Smart Business Solution. Localization : Uruguay - Xpande
 * Xpande. Created by Gabriel Vila on 11/29/20.
 */
public class VtaCpraProdSocio extends SvrProcess {

    private final String TABLA_REPORTE = "Z_RP_Trale_InvProdBP";

    private int adOrgID = 0;
    private int cBPartnerID = 0;
    private int mProductID = 0;
    private int cCurrencyID = 0;
    private Timestamp startDate = null;
    private Timestamp endDate = null;
    private String ReportType ="";
    private String ReportCurrencyType = "";

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
                    else if (name.trim().equalsIgnoreCase("M_Product_ID")){
                        this.mProductID = ((BigDecimal)para[i].getParameter()).intValueExact();
                    }
                    else if (name.trim().equalsIgnoreCase("C_Currency_ID")){
                        this.cCurrencyID = ((BigDecimal)para[i].getParameter()).intValueExact();
                    }
                    else if (name.equalsIgnoreCase("TipoRepVtaCpra")){
                        if (para[i].getParameter() != null) this.ReportType = ((String)para[i].getParameter()).trim();
                    }
                    else if (name.equalsIgnoreCase("TipoCurVtaCpra")){
                        if (para[i].getParameter() != null) this.ReportCurrencyType = ((String)para[i].getParameter()).trim();
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
     * Xpande. Created by Gabriel Vila on 9/11/17.
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

    private void getData(){

        String sql, action, whereClause;

        try{
            String tablaDatos = "ZV_Trale_VtaProdBP";

            action = " insert into " + TABLA_REPORTE + " (ad_client_id, ad_org_id, ad_user_id, m_product_id, c_currency_id, c_bpartner_id, " +
                    "codigobp, nombrebp, qtyentered, totalamt)" ;

            if (this.ReportType.equalsIgnoreCase("VENTA")){
                whereClause = "v.issotrx ='Y'";
            }
            else{
                whereClause = "v.issotrx ='N'";
                tablaDatos = "ZV_Trale_CpraProdBP";
            }
            whereClause += " and dateinvoiced between '" + this.startDate + "' and '" + this.endDate + "'";

            if (this.cBPartnerID > 0) whereClause += " and v.c_bpartner_id =" + this.cBPartnerID;
            if (this.mProductID > 0) whereClause += " and v.m_product_id =" + this.mProductID;


            String sumAction = "";
            if (this.ReportCurrencyType.equalsIgnoreCase("TODASTRX")){
                sumAction = " sum(v.qtyinvoiced), sum(v.linenetamtmt) ";
            }
            else if (this.ReportCurrencyType.equalsIgnoreCase("TODAS")){
                if (this.cCurrencyID == 142){
                    sumAction = " sum(v.qtyinvoiced), sum(v.linenetamtmn) ";
                }
                else{
                    sumAction = " sum(v.qtyinvoiced), sum(v.linenetamtme) ";
                    whereClause += " and v.c_currency_id in (142," + this.cCurrencyID + ")";
                }
            }

            sql = " select v.ad_client_id, v.ad_org_id, " + this.getAD_User_ID() + ", v.m_product_id, v.c_currency_id, " +
                    "v.c_bpartner_id, bp.value, bp.name2, " + sumAction +
                    " from " + tablaDatos + " v " +
                    " inner join c_bpartner bp on v.c_bpartner_id = bp.c_bpartner_id " +
                    " inner join m_product prod on v.m_product_id = prod.m_product_id "	+
                    " where " + whereClause + " and prod.value <>'redondeo' " +
                    " group by v.ad_client_id, v.ad_org_id, v.m_product_id, v.c_currency_id, v.c_bpartner_id, bp.value, bp.name2 ";

            DB.executeUpdateEx(action + sql, null);

        }
        catch (Exception e){
            throw new AdempiereException(e);
        }
    }

    private void updateData(){

        String sql = "";
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try{

            sql = " SELECT * " +
                    " FROM " + TABLA_REPORTE +
                    " WHERE ad_user_id =?" +
                    " ORDER BY m_product_id, c_currency_id, c_bpartner_id ";

            pstmt = DB.prepareStatement(sql, null);
            pstmt.setInt(1, this.getAD_User_ID());

            rs = pstmt.executeQuery ();

            String action = "";

            int mProductIDAux = 0;
            BigDecimal totalAmt = Env.ZERO;

            while (rs.next()){

                if (rs.getInt("m_product_id") != mProductIDAux){

                    mProductIDAux = rs.getInt("m_product_id");

                    // Obtengo total para este nuevo producto
                    totalAmt = DB.getSQLValueBDEx(null, " select sum(coalesce(qtyentered,0)) as total from " + TABLA_REPORTE
                            + " where ad_user_id =" + this.getAD_User_ID()
                            + " and m_product_id =" + mProductIDAux);
                }

                BigDecimal porcentaje = (Env.ONEHUNDRED.multiply(rs.getBigDecimal("qtyentered"))).divide(totalAmt, 2, RoundingMode.HALF_UP);

                action = " update " + TABLA_REPORTE
                        + " set percenttotal =" + porcentaje
                        + " where ad_user_id =" + this.getAD_User_ID()
                        + " and m_product_id =" + mProductIDAux
                        + " and c_currency_id =" + rs.getInt("c_currency_id")
                        + " and c_bpartner_id =" + rs.getInt("c_bpartner_id");

                DB.executeUpdateEx(action, null);
            }

        }
        catch (Exception e){
            throw new AdempiereException(e);
        }
        finally {
            DB.close(rs, pstmt);
        	rs = null; pstmt = null;
        }
    }

}
