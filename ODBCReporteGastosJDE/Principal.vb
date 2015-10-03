Imports System.Xml
Imports System.IO
Imports System.Data.SqlClient
Imports System.Data
Imports System.Net
Module Principal

    Sub Main()

        cargar_parametros()
        cadenaAS400_DTA = "DSN=SPI;uid=TRANSFTP;pwd=TRANSFTP;"
        transferirRegistrosAJde()

    End Sub

    Public conexionString As String = ""
    Dim lineaLogger As String
    Dim logger As StreamWriter
    Dim Conn400 As ADODB.Connection
    Dim Rst400 As ADODB.Recordset
    Dim SQL As String
    Dim CmdSQL As ADODB.Command
    Dim Cmd400 As ADODB.Command
    Dim cadenaAS400_DTA As String
    Dim server As String
    Dim database As String
    Dim uid As String
    Dim pwd As String

    Private Sub transferirRegistrosAJde()

        Dim estado As String
        Dim connSQL As New SqlConnection
        Dim cmdSQL As New SqlCommand
        connSQL.ConnectionString = conexionString
        connSQL.Open()

        Dim format As String = "dd/MM/yyyy"

        cmdSQL.Connection = connSQL
        cmdSQL.CommandText = "select id,anticipo,monto_gastado from cabecera_reporte where estado='APROBADO' and transferir='SI' and actualizadoJDE='NO'"
        Dim lrdSQL As SqlDataReader = cmdSQL.ExecuteReader()
        estado = ""
        While lrdSQL.Read()

            If lrdSQL("anticipo") = 0 Then
                SinAnticipo(lrdSQL("id"))
            Else
                If lrdSQL("monto_gastado") > 0 Then
                    ReembolsoEmpresa(lrdSQL("id"))
                Else
                    If lrdSQL("monto_gastado") < 0 Then
                        ReembolsoTrabajador(lrdSQL("id"))
                    Else
                        SinReembolso(lrdSQL("id"))
                    End If

                End If
            End If

        End While

        lrdSQL.Close()
        connSQL.Close()



    End Sub



    Private Sub SinAnticipo(ByVal id As Integer)

        Dim nxtEDBT As Long
        Dim nxtICU As Long
        Dim linea As Long
        Dim empresa As String
        Dim ficha As String
        Dim cantidad As Double
        Dim moneda As String
        Dim idCuentaContable As String
        Dim factura As String
        Dim observacion As String
        Dim nombre As String
        Dim fechaFactura As String
        Dim fechaProceso As String
        Dim hora As String
        Dim cuenta As String
        Dim subcuenta As String
        Dim subcuentaAuxiliar As String
        Dim idCuentaContableAux As String

        Dim connSQL As New SqlConnection
        Dim cmdSQL As New SqlCommand
        connSQL.ConnectionString = conexionString
        connSQL.Open()

        cmdSQL.Connection = connSQL
        cmdSQL.CommandText = "select cabecera_reporte.fecha,cabecera_reporte.id,abs(total-excedente) as monto_gastado,linea,company,auxiliar,moneda,referencia,observacion,cabecera_reporte.nombre,cabecera_reporte.fecha,detalle_reporte.centro_costo,concepto from cabecera_reporte,detalle_reporte,usuario where cabecera_reporte.id=detalle_reporte.id_cabecera and usuario.ficha=cabecera_reporte.ficha and transferir='SI' and actualizadoJDE='NO' and cabecera_reporte.id=" & id & " "
        Dim lrdSQL As SqlDataReader = cmdSQL.ExecuteReader()

        While lrdSQL.Read()

            nxtEDBT = obtenerCorrelativoEDBT()
            nxtICU = obtenerCorrelativoICU()

            linea = CDbl(lrdSQL("linea")) * 1000
            empresa = Trim(lrdSQL("company")) '"00300"
            ficha = Trim(lrdSQL("auxiliar")) '"11409"
            cantidad = Math.Round(CDbl(Math.Round(lrdSQL("monto_gastado"), 2)), 2) * 100 ' 20000 200 Bolivares
            moneda = Trim(lrdSQL("moneda"))
            idCuentaContable = "70015137"
            factura = Trim(lrdSQL("referencia"))
            observacion = Left(id & "-" & Trim(lrdSQL("observacion")), 30)
            nombre = Trim(lrdSQL("nombre"))
            fechaFactura = buscarFechaJuliana(CDate(lrdSQL("fecha")))
            fechaProceso = buscarFechaJuliana(CDate(Now))
            hora = Format(CDate(Now), "hhmmss")
            cuenta = Trim(lrdSQL("centro_costo"))
            subcuenta = buscarCuenta(lrdSQL("concepto")) '"6480"
            subcuentaAuxiliar = buscarSubCuenta(lrdSQL("concepto"))  '"0002"

            SQL = "INSERT INTO F0411Z1 ( " & _
            "VLEDUS,VLEDTY,VLEDSQ,VLEDTN,VLEDCT,VLEDLN,VLEDTS,VLEDFT,VLEDDT,VLEDER,VLEDDL,VLEDSP,VLEDTC,VLEDTR,VLEDBT,VLEDGL,VLEDDH,VLEDAN,VLKCO,VLDOC,VLDCT,VLSFX,VLSFXE,VLDCTA,VLAN8,VLPYE,VLSNTO,VLDIVJ,VLDSVJ,VLDDJ,VLDDNJ,VLDGJ,VLFY,VLCTRY,VLPN,VLCO,VLICU,VLICUT,VLDICJ,VLBALJ,VLPST,VLAG,VLAAP,VLADSC,VLADSA,VLATXA,VLATXN,VLSTAM,VLTXA1,VLEXR1,VLCRRM,VLCRCD,VLCRR,VLACR,VLFAP,VLCDS,VLCDSA,VLCTXA,VLCTXN,VLCTAM,VLGLC,VLGLBA,VLPOST,VLAM,VLAID2,VLMCU,VLOBJ,VLSUB,VLSBLT,VLSBL,VLBAID,VLPTC,VLVOD,VLOKCO,VLODCT,VLODOC,VLOSFX,VLCRC,VLVINV,VLPKCO,VLPO,VLPDCT,VLLNID,VLSFXO,VLOPSQ,VLVR01,VLUNIT,VLMCU2,VLRMK,VLRF,VLDRF,VLCTL,VLFNLP,VLU,VLUM,VLPYIN,VLTXA3,VLEXR3,VLRP1,VLRP2,VLRP3,VLAC07,VLTNN,VLDMCD,VLITM,VLHCRR,VLHDGJ,VLURC1,VLURDT,VLURAT,VLURAB,VLURRF,VLTORG,VLUSER,VLPID,VLUPMJ,VLUPMT,VLJOBN,VLDIM,VLDID,VLDIY,VLDI#,VLDSVM,VLDSVD,VLDSVY,VLDSV#,VLDDM,VLDDD,VLDDY,VLDD#,VLDDNM,VLDDND,VLDDNY,VLDDN#,VLDGM,VLDGD,VLDGY,VLDG#,VLDICM,VLDICD,VLDICY,VLDIC#,VLHDGM,VLHDGD,VLHDGY,VLHDG#,VLDOCM,VLTNST,VLYC01,VLYC02,VLYC03,VLYC04,VLYC05,VLYC06," & _
            "VLYC07 , VLYC08,VLYC09,VLYC10,VLDTXS,VLBCRC,VLATAD,VLCTAD,VLNRTA,VLFNRT,VLTAXP,VLPRGF,VLGFL5,VLGFL6,VLGAM1,VLGAM2,VLGEN4,VLGEN5,VLWTAD,VLWTAF,VLSMMF,VLPYWP,VLPWPG,VLNETST) " & _
                  " VALUES (" & _
                  "'AMAZING','','0','1','','" & linea & "','','','0','B','0','0','A','V','" & nxtEDBT & "','','1','0','" & empresa & "','0','','','0','','" & ficha & "','" & ficha & "','0','" & fechaFactura & "','" & fechaProceso & "','" & fechaFactura & "','" & fechaFactura & "','" & fechaProceso & "','0','20','0','" & empresa & "','0','V','0','Y','A','" & cantidad & "','" & cantidad & "','0','0','0','" & cantidad & "','0','IVA','E','D','" & moneda & "','0','0','0','0','0','0','0','0','EMP','70015137','','2','','        3009','','','','','','1','','',''," & id & ",'','','" & factura & "','','','','0','','0','','','','" & Left(observacion, 30) & "','','0','','','0','','','','','','','','N','','','0','0','0','','0','0','0','','AMAZING','AMAZING','ZP0411Z1','" & fechaProceso & "','" & hora & "','NTHTTP01VE','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','','','','','','','','','','','','','','0','0','0','0','','','','','0','0','','','0','0',''," & _
                  "'','0','')"

            insertInto400(SQL)

            '------------------------------------------------------------------------------------------

            idCuentaContableAux = obtenerIdCuenta(cuenta, subcuenta, subcuentaAuxiliar)

            SQL = "INSERT INTO F0911Z1 ( " & _
            "VNEDUS,VNEDTY,VNEDSQ,VNEDTN,VNEDCT,VNEDLN,VNEDTS,VNEDFT,VNEDDT,VNEDER,VNEDDL,VNEDSP,VNEDTC,VNEDTR,VNEDBT,VNEDGL,VNEDAN,VNKCO,VNDCT,VNDOC,VNDGJ,VNJELN,VNEXTL,VNPOST,VNICU,VNICUT,VNDICJ,VNDSYJ,VNTICU,VNCO,VNANI,VNAM,VNAID,VNMCU,VNOBJ,VNSUB,VNSBL,VNSBLT,VNLT,VNPN,VNCTRY,VNFY,VNFQ,VNCRCD,VNCRR,VNHCRR,VNHDGJ,VNAA,VNU,VNUM,VNGLC,VNRE,VNEXA,VNEXR,VNR1,VNR2,VNR3,VNSFX,VNODOC,VNODCT,VNOSFX,VNPKCO,VNOKCO,VNPDCT,VNAN8,VNCN,VNDKJ,VNDKC,VNASID,VNBRE,VNRCND,VNSUMM,VNPRGE,VNTNN,VNALT1,VNALT2,VNALT3,VNALT4,VNALT5,VNALT6,VNALT7,VNALT8,VNALT9,VNALT0,VNALTT,VNALTU,VNALTV,VNALTW,VNALTX,VNALTZ,VNDLNA,VNCFF1,VNCFF2,VNASM,VNBC,VNVINV,VNIVD,VNWR01,VNPO,VNPSFX,VNDCTO,VNLNID,VNWY,VNWN,VNFNLP,VNOPSQ,VNJBCD,VNJBST,VNHMCU,VNDOI,VNALID,VNALTY,VNDSVJ,VNTORG,VNREG#,VNPYID,VNUSER,VNPID,VNJOBN,VNUPMJ,VNUPMT,VNCRRM,VNACR,VNDGM,VNDGD,VNDGY,VNDG#,VNDICM,VNDICD,VNDICY,VNDIC#,VNDSYM,VNDSYD,VNDSYY,VNDSY#,VNDKM,VNDKD,VNDKY,VNDK#,VNDSVM,VNDSVD,VNDSVY,VNDSV#,VNHDGM,VNHDGD,VNHDGY,VNHDG#,VNDKCM,VNDKCD,VNDKCY,VNDKC#,VNIVDM,VNIVDD,VNIVDY,VNIVD#," & _
            "VNABR1 , VNABR2, VNABR3, VNABR4, VNABT1, VNABT2, VNABT3, VNABT4, VNITM, VNPM01, VNPM02, VNPM03, VNPM04, VNPM05, VNPM06, VNPM07, VNPM08, VNPM09, VNPM10, VNBCRC, VNEXR1, VNTXA1, VNTXITM, VNACTB, VNSTAM, VNCTAM, VNAG, VNAGF, VNTKTX, VNDLNID, VNCKNU)" & _
                  " VALUES (" & _
                  "'AMAZING','','0','1','','" & linea & "','','','0','B','0','0','A','V','" & nxtEDBT & "','','0','','','0','" & fechaProceso & "','0','','','" & nxtICU & "','V','0','0','0','" & empresa & "','" & cuenta & "." & subcuenta & "." & subcuentaAuxiliar & "','2','" & idCuentaContableAux & "','','','','','','AA','0','20','0','','" & moneda & "','0','0','0','" & cantidad & "','0','','','','" & Left(nombre, 30) & "','" & Left(observacion, 30) & "','','','','','" & id & "','','','','','','" & ficha & "','','0','0','','','','','','','','','','','','','','','','','','','','','','','','','','','','" & factura & "','" & fechaFactura & "','','','','','0','0','0','','0','','','','0','','','" & fechaProceso & "','','0','0','AMAZING','ZP0411Z1','NTHTTP01VE','" & fechaProceso & "'" & _
                  ",'" & hora & "','D','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','','','','','','','','','0','','','','','','','','','','','','','','0','','0','0','0','0','0','0','')"

            insertInto400(SQL)

            '------------------------------------------------------------------------------------------

            updateIntoSQLServer("update cabecera_reporte set actualizadoJDE='SI' where id=" & lrdSQL("id") & "")

        End While

        lrdSQL.Close()
        connSQL.Close()

    End Sub


    Private Sub ReembolsoEmpresa(ByVal id As Integer)

        Dim nxtEDBT As Long
        Dim nxtICU As Long
        Dim linea As Long
        Dim empresa As String
        Dim ficha As String
        Dim cantidad As Double
        Dim moneda As String
        Dim idCuentaContable As String
        Dim factura As String
        Dim observacion As String
        Dim nombre As String
        Dim fechaFactura As String
        Dim fechaProceso As String
        Dim hora As String
        Dim cuenta As String
        Dim subcuenta As String
        Dim subcuentaAuxiliar As String
        Dim idCuentaContableAux As String
        Dim factor As Long

        Dim connSQL As New SqlConnection
        Dim cmdSQL As New SqlCommand
        connSQL.ConnectionString = conexionString
        connSQL.Open()

        cmdSQL.Connection = connSQL
        cmdSQL.CommandText = "select cabecera_reporte.fecha,cabecera_reporte.id,abs(total-excedente) as monto_gastado,abs(monto_gastado) as total_gasto,anticipo,linea,company,auxiliar,moneda,referencia,observacion,cabecera_reporte.nombre,cabecera_reporte.fecha,detalle_reporte.centro_costo,concepto from cabecera_reporte,detalle_reporte,usuario where cabecera_reporte.id=detalle_reporte.id_cabecera and usuario.ficha=cabecera_reporte.ficha and transferir='SI' and actualizadoJDE='NO' and cabecera_reporte.id=" & id & " "
        Dim lrdSQL As SqlDataReader = cmdSQL.ExecuteReader()

        factor = 1000

        While lrdSQL.Read()

            nxtEDBT = obtenerCorrelativoEDBT()
            nxtICU = obtenerCorrelativoICU()

            linea = CDbl(lrdSQL("linea")) * 1000
            empresa = Trim(lrdSQL("company")) '"00300"
            ficha = Trim(lrdSQL("auxiliar")) '"11409"
            cantidad = Math.Round(CDbl(Math.Round(lrdSQL("monto_gastado"), 2)), 2) * 100 ' 20000 200 Bolivares
            moneda = Trim(lrdSQL("moneda"))
            idCuentaContable = "70015137"
            factura = Trim(lrdSQL("referencia"))
            observacion = Left(id & "-" & Trim(lrdSQL("observacion")), 30)
            nombre = Trim(lrdSQL("nombre"))
            fechaFactura = buscarFechaJuliana(CDate(lrdSQL("fecha")))
            fechaProceso = buscarFechaJuliana(CDate(Now))
            hora = Format(CDate(Now), "hhmmss")
            cuenta = Trim(lrdSQL("centro_costo"))
            subcuenta = buscarCuenta(lrdSQL("concepto")) '"6480"
            subcuentaAuxiliar = buscarSubCuenta(lrdSQL("concepto"))  '"0002"


            idCuentaContableAux = obtenerIdCuenta(cuenta, subcuenta, subcuentaAuxiliar)
            SQL = "INSERT INTO F0911Z1 ( " & _
            "VNEDUS,VNEDTY,VNEDSQ,VNEDTN,VNEDCT,VNEDLN,VNEDTS,VNEDFT,VNEDDT,VNEDER,VNEDDL,VNEDSP,VNEDTC,VNEDTR,VNEDBT,VNEDGL,VNEDAN,VNKCO,VNDCT,VNDOC,VNDGJ,VNJELN,VNEXTL,VNPOST,VNICU,VNICUT,VNDICJ,VNDSYJ,VNTICU,VNCO,VNANI,VNAM,VNAID,VNMCU,VNOBJ,VNSUB,VNSBL,VNSBLT,VNLT,VNPN,VNCTRY,VNFY,VNFQ,VNCRCD,VNCRR,VNHCRR,VNHDGJ,VNAA,VNU,VNUM,VNGLC,VNRE,VNEXA,VNEXR,VNR1,VNR2,VNR3,VNSFX,VNODOC,VNODCT,VNOSFX,VNPKCO,VNOKCO,VNPDCT,VNAN8,VNCN,VNDKJ,VNDKC,VNASID,VNBRE,VNRCND,VNSUMM,VNPRGE,VNTNN,VNALT1,VNALT2,VNALT3,VNALT4,VNALT5,VNALT6,VNALT7,VNALT8,VNALT9,VNALT0,VNALTT,VNALTU,VNALTV,VNALTW,VNALTX,VNALTZ,VNDLNA,VNCFF1,VNCFF2,VNASM,VNBC,VNVINV,VNIVD,VNWR01,VNPO,VNPSFX,VNDCTO,VNLNID,VNWY,VNWN,VNFNLP,VNOPSQ,VNJBCD,VNJBST,VNHMCU,VNDOI,VNALID,VNALTY,VNDSVJ,VNTORG,VNREG#,VNPYID,VNUSER,VNPID,VNJOBN,VNUPMJ,VNUPMT,VNCRRM,VNACR,VNDGM,VNDGD,VNDGY,VNDG#,VNDICM,VNDICD,VNDICY,VNDIC#,VNDSYM,VNDSYD,VNDSYY,VNDSY#,VNDKM,VNDKD,VNDKY,VNDK#,VNDSVM,VNDSVD,VNDSVY,VNDSV#,VNHDGM,VNHDGD,VNHDGY,VNHDG#,VNDKCM,VNDKCD,VNDKCY,VNDKC#,VNIVDM,VNIVDD,VNIVDY,VNIVD#," & _
            "VNABR1 , VNABR2, VNABR3, VNABR4, VNABT1, VNABT2, VNABT3, VNABT4, VNITM, VNPM01, VNPM02, VNPM03, VNPM04, VNPM05, VNPM06, VNPM07, VNPM08, VNPM09, VNPM10, VNBCRC, VNEXR1, VNTXA1, VNTXITM, VNACTB, VNSTAM, VNCTAM, VNAG, VNAGF, VNTKTX, VNDLNID, VNCKNU)" & _
                  " VALUES (" & _
                  "'AMAZING','','0','1','','" & linea & "','','','0','B','0','0','A','J','" & nxtEDBT & "','','0','','UL','0','" & fechaProceso & "','0','','','" & nxtICU & "','G','0','0','0','" & empresa & "','" & cuenta & "." & subcuenta & "." & subcuentaAuxiliar & "','2','" & idCuentaContableAux & "','','','','','','AA','0','20','0','','" & moneda & "','0','0','0','" & cantidad & "','0','','','','" & Left(nombre, 30) & "','" & Left(observacion, 30) & "','','','','','" & id & "','','','','','','" & ficha & "','','0','0','','','','','','','','','','','','','','','','','','','','','','','','','','','','" & factura & "','" & fechaFactura & "','','','','','0','0','0','','0','','','','0','','','" & fechaProceso & "','','0','0','AMAZING','ZP0411Z1','NTHTTP01VE','" & fechaProceso & "','" & hora & "','D'" & _
                  ",'0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','','','','','','','','','0','','','','','','','','','','','','','','0','','0','0','0','0','0','0','')"

            insertInto400(SQL)

            idCuentaContableAux = obtenerIdCuenta(cuenta, subcuenta, subcuentaAuxiliar)

            '------------------------------------------------------------------------------------------
            factor = factor + 1000
            linea = CDbl(lrdSQL("linea")) * factor
            cantidad = Math.Round(CDbl(lrdSQL("total_gasto")), 2) * 100
            cuenta = "3009" ' CUENTA BANCO
            subcuenta = "1105"
            subcuentaAuxiliar = "1017"

            idCuentaContableAux = obtenerIdCuenta(cuenta, subcuenta, subcuentaAuxiliar)

            SQL = "INSERT INTO F0911Z1 ( " & _
            "VNEDUS,VNEDTY,VNEDSQ,VNEDTN,VNEDCT,VNEDLN,VNEDTS,VNEDFT,VNEDDT,VNEDER,VNEDDL,VNEDSP,VNEDTC,VNEDTR,VNEDBT,VNEDGL,VNEDAN,VNKCO,VNDCT,VNDOC,VNDGJ,VNJELN,VNEXTL,VNPOST,VNICU,VNICUT,VNDICJ,VNDSYJ,VNTICU,VNCO,VNANI,VNAM,VNAID,VNMCU,VNOBJ,VNSUB,VNSBL,VNSBLT,VNLT,VNPN,VNCTRY,VNFY,VNFQ,VNCRCD,VNCRR,VNHCRR,VNHDGJ,VNAA,VNU,VNUM,VNGLC,VNRE,VNEXA,VNEXR,VNR1,VNR2,VNR3,VNSFX,VNODOC,VNODCT,VNOSFX,VNPKCO,VNOKCO,VNPDCT,VNAN8,VNCN,VNDKJ,VNDKC,VNASID,VNBRE,VNRCND,VNSUMM,VNPRGE,VNTNN,VNALT1,VNALT2,VNALT3,VNALT4,VNALT5,VNALT6,VNALT7,VNALT8,VNALT9,VNALT0,VNALTT,VNALTU,VNALTV,VNALTW,VNALTX,VNALTZ,VNDLNA,VNCFF1,VNCFF2,VNASM,VNBC,VNVINV,VNIVD,VNWR01,VNPO,VNPSFX,VNDCTO,VNLNID,VNWY,VNWN,VNFNLP,VNOPSQ,VNJBCD,VNJBST,VNHMCU,VNDOI,VNALID,VNALTY,VNDSVJ,VNTORG,VNREG#,VNPYID,VNUSER,VNPID,VNJOBN,VNUPMJ,VNUPMT,VNCRRM,VNACR,VNDGM,VNDGD,VNDGY,VNDG#,VNDICM,VNDICD,VNDICY,VNDIC#,VNDSYM,VNDSYD,VNDSYY,VNDSY#,VNDKM,VNDKD,VNDKY,VNDK#,VNDSVM,VNDSVD,VNDSVY,VNDSV#,VNHDGM,VNHDGD,VNHDGY,VNHDG#,VNDKCM,VNDKCD,VNDKCY,VNDKC#,VNIVDM,VNIVDD,VNIVDY,VNIVD#," & _
            "VNABR1 , VNABR2, VNABR3, VNABR4, VNABT1, VNABT2, VNABT3, VNABT4, VNITM, VNPM01, VNPM02, VNPM03, VNPM04, VNPM05, VNPM06, VNPM07, VNPM08, VNPM09, VNPM10, VNBCRC, VNEXR1, VNTXA1, VNTXITM, VNACTB, VNSTAM, VNCTAM, VNAG, VNAGF, VNTKTX, VNDLNID, VNCKNU)" & _
                  " VALUES (" & _
                  "'AMAZING','','0','1','','" & linea & "','','','0','B','0','0','A','J','" & nxtEDBT & "','','0','','UL','0','" & fechaProceso & "','0','','','" & nxtICU & "','G','0','0','0','" & empresa & "','" & cuenta & "." & subcuenta & "." & subcuentaAuxiliar & "','2','" & idCuentaContableAux & "','','','','','','AA','0','20','0','','" & moneda & "','0','0','0','" & cantidad & "','0','','','','" & Left(nombre, 30) & "','" & Left(observacion, 30) & "','','','','','" & id & "','','','','','','" & ficha & "','','0','0','','','','','','','','','','','','','','','','','','','','','','','','','','','','" & factura & "','" & fechaFactura & "','','','','','0','0','0','','0','','','','0','','','" & fechaProceso & "','','0','0','AMAZING','ZP0411Z1','NTHTTP01VE','" & fechaProceso & "','" & hora & "','D'" & _
                  ",'0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','','','','','','','','','0','','','','','','','','','','','','','','0','','0','0','0','0','0','0','')"

            insertInto400(SQL)

            '------------------------------------------------------------------------------------------


            factor = factor + 1000
            linea = CDbl(lrdSQL("linea")) * factor
            cantidad = Math.Round(CDbl(lrdSQL("anticipo")), 2) * -1 * 100 ' ANTICIPO
            cuenta = "3009"
            subcuenta = "1260"
            subcuentaAuxiliar = "1003"

            idCuentaContableAux = obtenerIdCuenta(cuenta, subcuenta, subcuentaAuxiliar)

            SQL = "INSERT INTO F0911Z1 ( " & _
            "VNEDUS,VNEDTY,VNEDSQ,VNEDTN,VNEDCT,VNEDLN,VNEDTS,VNEDFT,VNEDDT,VNEDER,VNEDDL,VNEDSP,VNEDTC,VNEDTR,VNEDBT,VNEDGL,VNEDAN,VNKCO,VNDCT,VNDOC,VNDGJ,VNJELN,VNEXTL,VNPOST,VNICU,VNICUT,VNDICJ,VNDSYJ,VNTICU,VNCO,VNANI,VNAM,VNAID,VNMCU,VNOBJ,VNSUB,VNSBL,VNSBLT,VNLT,VNPN,VNCTRY,VNFY,VNFQ,VNCRCD,VNCRR,VNHCRR,VNHDGJ,VNAA,VNU,VNUM,VNGLC,VNRE,VNEXA,VNEXR,VNR1,VNR2,VNR3,VNSFX,VNODOC,VNODCT,VNOSFX,VNPKCO,VNOKCO,VNPDCT,VNAN8,VNCN,VNDKJ,VNDKC,VNASID,VNBRE,VNRCND,VNSUMM,VNPRGE,VNTNN,VNALT1,VNALT2,VNALT3,VNALT4,VNALT5,VNALT6,VNALT7,VNALT8,VNALT9,VNALT0,VNALTT,VNALTU,VNALTV,VNALTW,VNALTX,VNALTZ,VNDLNA,VNCFF1,VNCFF2,VNASM,VNBC,VNVINV,VNIVD,VNWR01,VNPO,VNPSFX,VNDCTO,VNLNID,VNWY,VNWN,VNFNLP,VNOPSQ,VNJBCD,VNJBST,VNHMCU,VNDOI,VNALID,VNALTY,VNDSVJ,VNTORG,VNREG#,VNPYID,VNUSER,VNPID,VNJOBN,VNUPMJ,VNUPMT,VNCRRM,VNACR,VNDGM,VNDGD,VNDGY,VNDG#,VNDICM,VNDICD,VNDICY,VNDIC#,VNDSYM,VNDSYD,VNDSYY,VNDSY#,VNDKM,VNDKD,VNDKY,VNDK#,VNDSVM,VNDSVD,VNDSVY,VNDSV#,VNHDGM,VNHDGD,VNHDGY,VNHDG#,VNDKCM,VNDKCD,VNDKCY,VNDKC#,VNIVDM,VNIVDD,VNIVDY,VNIVD#," & _
            "VNABR1 , VNABR2, VNABR3, VNABR4, VNABT1, VNABT2, VNABT3, VNABT4, VNITM, VNPM01, VNPM02, VNPM03, VNPM04, VNPM05, VNPM06, VNPM07, VNPM08, VNPM09, VNPM10, VNBCRC, VNEXR1, VNTXA1, VNTXITM, VNACTB, VNSTAM, VNCTAM, VNAG, VNAGF, VNTKTX, VNDLNID, VNCKNU)" & _
                  " VALUES (" & _
                  "'AMAZING','','0','1','','" & linea & "','','','0','B','0','0','A','J','" & nxtEDBT & "','','0','','UL','0','" & fechaProceso & "','0','','','" & nxtICU & "','G','0','0','0','" & empresa & "','" & cuenta & "." & subcuenta & "." & subcuentaAuxiliar & "','2','" & idCuentaContableAux & "','','','','" & ficha & "','A','AA','0','20','0','','" & moneda & "','0','0','0','" & cantidad & "','0','','','','" & Left(nombre, 30) & "','" & Left(observacion, 30) & "','','','','','" & id & "','','','','','','" & ficha & "','','0','0','','','','','','','','','','','','','','','','','','','','','','','','','','','','" & factura & "','" & fechaFactura & "','','','','','0','0','0','','0','','','','0','','','" & fechaProceso & "','','0','0','AMAZING','ZP0411Z1','NTHTTP01VE','" & fechaProceso & "','" & hora & "','D'" & _
                  ",'0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','','','','','','','','','0','','','','','','','','','','','','','','0','','0','0','0','0','0','0','')"

            insertInto400(SQL)

            '------------------------------------------------------------------------------------------

            updateIntoSQLServer("update cabecera_reporte set actualizadoJDE='SI' where id=" & lrdSQL("id") & "")

        End While

        lrdSQL.Close()
        connSQL.Close()

    End Sub

    Private Sub ReembolsoTrabajador(ByVal id As Integer)

        Dim nxtEDBT As Long
        Dim nxtICU As Long
        Dim linea As Long
        Dim empresa As String
        Dim ficha As String
        Dim cantidad As Double
        Dim moneda As String
        Dim idCuentaContable As String
        Dim factura As String
        Dim observacion As String
        Dim nombre As String
        Dim fechaFactura As String
        Dim fechaProceso As String
        Dim hora As String
        Dim cuenta As String
        Dim subcuenta As String
        Dim subcuentaAuxiliar As String
        Dim idCuentaContableAux As String
        Dim factor As Long

        Dim connSQL As New SqlConnection
        Dim cmdSQL As New SqlCommand
        connSQL.ConnectionString = conexionString
        connSQL.Open()

        cmdSQL.Connection = connSQL
        cmdSQL.CommandText = "select cabecera_reporte.fecha,cabecera_reporte.id,abs(total-excedente) as monto_gastado,abs(monto_gastado) as total_gasto,anticipo,linea,company,auxiliar,moneda,referencia,observacion,cabecera_reporte.nombre,cabecera_reporte.fecha,detalle_reporte.centro_costo,concepto from cabecera_reporte,detalle_reporte,usuario where cabecera_reporte.id=detalle_reporte.id_cabecera and usuario.ficha=cabecera_reporte.ficha and transferir='SI' and actualizadoJDE='NO' and cabecera_reporte.id=" & id & " "
        Dim lrdSQL As SqlDataReader = cmdSQL.ExecuteReader()

        factor = 1000

        While lrdSQL.Read()

            nxtEDBT = obtenerCorrelativoEDBT()
            nxtICU = obtenerCorrelativoICU()

            linea = CDbl(lrdSQL("linea")) * 1000
            empresa = Trim(lrdSQL("company")) '"00300"
            ficha = Trim(lrdSQL("auxiliar")) '"11409"
            cantidad = Math.Round(CDbl(Math.Round(lrdSQL("monto_gastado"), 2)), 2) * 100 ' 20000 200 Bolivares
            moneda = Trim(lrdSQL("moneda"))
            idCuentaContable = "70015137"
            factura = Trim(lrdSQL("referencia"))
            observacion = Left(id & "-" & Trim(lrdSQL("observacion")), 30)
            nombre = Trim(lrdSQL("nombre"))
            fechaFactura = buscarFechaJuliana(CDate(lrdSQL("fecha")))
            fechaProceso = buscarFechaJuliana(CDate(Now))
            hora = Format(CDate(Now), "hhmmss")
            cuenta = Trim(lrdSQL("centro_costo"))
            subcuenta = buscarCuenta(lrdSQL("concepto")) '"6480"
            subcuentaAuxiliar = buscarSubCuenta(lrdSQL("concepto"))  '"0002"


            linea = CDbl(lrdSQL("linea")) * factor
            'cantidad = Round((CDbl(RstSQLAS!anticipo) - CDbl(RstSQLAS!total_gasto)), 2) * 100 '100000 ' 20000 200 Bolivares
            cantidad = Math.Round((CDbl(lrdSQL("total_gasto"))), 2) * 100 '100000 ' 20000 200 Bolivares
            cuenta = "3009"
            subcuenta = "2210" ' CTAS X COBRAR
            subcuentaAuxiliar = "1003"


            SQL = "INSERT INTO F0411Z1 ( " & _
            "VLEDUS,VLEDTY,VLEDSQ,VLEDTN,VLEDCT,VLEDLN,VLEDTS,VLEDFT,VLEDDT,VLEDER,VLEDDL,VLEDSP,VLEDTC,VLEDTR,VLEDBT,VLEDGL,VLEDDH,VLEDAN,VLKCO,VLDOC,VLDCT,VLSFX,VLSFXE,VLDCTA,VLAN8,VLPYE,VLSNTO,VLDIVJ,VLDSVJ,VLDDJ,VLDDNJ,VLDGJ,VLFY,VLCTRY,VLPN,VLCO,VLICU,VLICUT,VLDICJ,VLBALJ,VLPST,VLAG,VLAAP,VLADSC,VLADSA,VLATXA,VLATXN,VLSTAM,VLTXA1,VLEXR1,VLCRRM,VLCRCD,VLCRR,VLACR,VLFAP,VLCDS,VLCDSA,VLCTXA,VLCTXN,VLCTAM,VLGLC,VLGLBA,VLPOST,VLAM,VLAID2,VLMCU,VLOBJ,VLSUB,VLSBLT,VLSBL,VLBAID,VLPTC,VLVOD,VLOKCO,VLODCT,VLODOC,VLOSFX,VLCRC,VLVINV,VLPKCO,VLPO,VLPDCT,VLLNID,VLSFXO,VLOPSQ,VLVR01,VLUNIT,VLMCU2,VLRMK,VLRF,VLDRF,VLCTL,VLFNLP,VLU,VLUM,VLPYIN,VLTXA3,VLEXR3,VLRP1,VLRP2,VLRP3,VLAC07,VLTNN,VLDMCD,VLITM,VLHCRR,VLHDGJ,VLURC1,VLURDT,VLURAT,VLURAB,VLURRF,VLTORG,VLUSER,VLPID,VLUPMJ,VLUPMT,VLJOBN,VLDIM,VLDID,VLDIY,VLDI#,VLDSVM,VLDSVD,VLDSVY,VLDSV#,VLDDM,VLDDD,VLDDY,VLDD#,VLDDNM,VLDDND,VLDDNY,VLDDN#,VLDGM,VLDGD,VLDGY,VLDG#,VLDICM,VLDICD,VLDICY,VLDIC#,VLHDGM,VLHDGD,VLHDGY,VLHDG#,VLDOCM,VLTNST,VLYC01,VLYC02,VLYC03,VLYC04,VLYC05,VLYC06," & _
            "VLYC07 , VLYC08,VLYC09,VLYC10,VLDTXS,VLBCRC,VLATAD,VLCTAD,VLNRTA,VLFNRT,VLTAXP,VLPRGF,VLGFL5,VLGFL6,VLGAM1,VLGAM2,VLGEN4,VLGEN5,VLWTAD,VLWTAF,VLSMMF,VLPYWP,VLPWPG,VLNETST) " & _
                  " VALUES (" & _
                  "'AMAZING','','0','1','','" & linea & "','','','0','B','0','0','A','V','" & nxtEDBT & "','','1','0','" & empresa & "','0','','','0','','" & ficha & "','" & ficha & "','0','" & fechaFactura & "','" & fechaProceso & "','" & fechaFactura & "','" & fechaFactura & "','" & fechaProceso & "','0','20','0','" & empresa & "','0','V','0','Y','A','" & cantidad & "','" & cantidad & "','0','0','0','" & cantidad & "','0','IVA','E','D','" & moneda & "','0','0','0','0','0','0','0','0','EMP','70015137','','2','','        3009','','','','','','1','','','','" & id & "','','','" & factura & "','','','','0','','0','','','','" & observacion & "','','0','','','0','','','','','','','','N','','','0','0','0','','0','0','0','','AMAZING','AMAZING','ZP0411Z1','" & fechaProceso & "','" & hora & "','NTHTTP01VE','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','','','','','','','','','','','','','','0','0','0','0','','','','','0','0','','','0','0','','','0','')"

            insertInto400(SQL)

            '------------------------------------------------------------------------------------------

            factor = factor + 1000
            linea = CDbl(lrdSQL("linea")) * factor
            'cantidad = Round(CDbl(RstSQLAS!anticipo), 2) * 100 ' 20000 200 Bolivares
            cantidad = Math.Round((CDbl(lrdSQL("anticipo")) + CDbl(lrdSQL("total_gasto"))), 2) * 100
            cuenta = Trim(lrdSQL("centro_costo"))
            subcuenta = buscarCuenta(lrdSQL("concepto")) '"6480"
            subcuentaAuxiliar = buscarSubCuenta(lrdSQL("concepto")) '"0002"

            ' cuenta = "3009"
            ' subcuenta = "2210"
            ' subcuentaAuxiliar = "1003"

            idCuentaContableAux = obtenerIdCuenta(cuenta, subcuenta, subcuentaAuxiliar)

            SQL = "INSERT INTO F0911Z1 ( " & _
            "VNEDUS,VNEDTY,VNEDSQ,VNEDTN,VNEDCT,VNEDLN,VNEDTS,VNEDFT,VNEDDT,VNEDER,VNEDDL,VNEDSP,VNEDTC,VNEDTR,VNEDBT,VNEDGL,VNEDAN,VNKCO,VNDCT,VNDOC,VNDGJ,VNJELN,VNEXTL,VNPOST,VNICU,VNICUT,VNDICJ,VNDSYJ,VNTICU,VNCO,VNANI,VNAM,VNAID,VNMCU,VNOBJ,VNSUB,VNSBL,VNSBLT,VNLT,VNPN,VNCTRY,VNFY,VNFQ,VNCRCD,VNCRR,VNHCRR,VNHDGJ,VNAA,VNU,VNUM,VNGLC,VNRE,VNEXA,VNEXR,VNR1,VNR2,VNR3,VNSFX,VNODOC,VNODCT,VNOSFX,VNPKCO,VNOKCO,VNPDCT,VNAN8,VNCN,VNDKJ,VNDKC,VNASID,VNBRE,VNRCND,VNSUMM,VNPRGE,VNTNN,VNALT1,VNALT2,VNALT3,VNALT4,VNALT5,VNALT6,VNALT7,VNALT8,VNALT9,VNALT0,VNALTT,VNALTU,VNALTV,VNALTW,VNALTX,VNALTZ,VNDLNA,VNCFF1,VNCFF2,VNASM,VNBC,VNVINV,VNIVD,VNWR01,VNPO,VNPSFX,VNDCTO,VNLNID,VNWY,VNWN,VNFNLP,VNOPSQ,VNJBCD,VNJBST,VNHMCU,VNDOI,VNALID,VNALTY,VNDSVJ,VNTORG,VNREG#,VNPYID,VNUSER,VNPID,VNJOBN,VNUPMJ,VNUPMT,VNCRRM,VNACR,VNDGM,VNDGD,VNDGY,VNDG#,VNDICM,VNDICD,VNDICY,VNDIC#,VNDSYM,VNDSYD,VNDSYY,VNDSY#,VNDKM,VNDKD,VNDKY,VNDK#,VNDSVM,VNDSVD,VNDSVY,VNDSV#,VNHDGM,VNHDGD,VNHDGY,VNHDG#,VNDKCM,VNDKCD,VNDKCY,VNDKC#,VNIVDM,VNIVDD,VNIVDY,VNIVD#," & _
            "VNABR1 , VNABR2, VNABR3, VNABR4, VNABT1, VNABT2, VNABT3, VNABT4, VNITM, VNPM01, VNPM02, VNPM03, VNPM04, VNPM05, VNPM06, VNPM07, VNPM08, VNPM09, VNPM10, VNBCRC, VNEXR1, VNTXA1, VNTXITM, VNACTB, VNSTAM, VNCTAM, VNAG, VNAGF, VNTKTX, VNDLNID, VNCKNU)" & _
                  " VALUES (" & _
                  "'AMAZING','','0','1','','" & linea & "','','','0','B','0','0','A','V','" & nxtEDBT & "','','0','','','0','" & fechaProceso & "','0','','','" & nxtICU & "','V','0','0','0','" & empresa & "','" & cuenta & "." & subcuenta & "." & subcuentaAuxiliar & "','2','" & idCuentaContableAux & "','','','','','','AA','0','20','0','','" & moneda & "','0','0','0','" & cantidad & "','0','','','','" & Left(nombre, 30) & "','" & Left(observacion, 30) & "','','','','','" & id & "','','','','','','" & ficha & "','','0','0','','','','','','','','','','','','','','','','','','','','','','','','','','','','" & factura & "','" & fechaFactura & "','','','','','0','0','0','','0','','','','0','','','" & fechaProceso & "','','0','0','AMAZING','ZP0411Z1','NTHTTP01VE','" & fechaProceso & "','" & hora & "','D'" & _
                  ",'0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','','','','','','','','','0','','','','','','','','','','','','','','0','','0','0','0','0','0','0','')"

            insertInto400(SQL)

            '----------------------------------------------------------------------------------------------

            factor = factor + 1000
            linea = CDbl(lrdSQL("linea")) * factor
            cantidad = Math.Round(lrdSQL("anticipo"), 2) * -1 * 100 ' 20000 200 Bolivares
            cuenta = "3009"
            subcuenta = "1260"
            subcuentaAuxiliar = "1003"

            idCuentaContableAux = obtenerIdCuenta(cuenta, subcuenta, subcuentaAuxiliar)

            SQL = "INSERT INTO F0911Z1 ( " & _
            "VNEDUS,VNEDTY,VNEDSQ,VNEDTN,VNEDCT,VNEDLN,VNEDTS,VNEDFT,VNEDDT,VNEDER,VNEDDL,VNEDSP,VNEDTC,VNEDTR,VNEDBT,VNEDGL,VNEDAN,VNKCO,VNDCT,VNDOC,VNDGJ,VNJELN,VNEXTL,VNPOST,VNICU,VNICUT,VNDICJ,VNDSYJ,VNTICU,VNCO,VNANI,VNAM,VNAID,VNMCU,VNOBJ,VNSUB,VNSBL,VNSBLT,VNLT,VNPN,VNCTRY,VNFY,VNFQ,VNCRCD,VNCRR,VNHCRR,VNHDGJ,VNAA,VNU,VNUM,VNGLC,VNRE,VNEXA,VNEXR,VNR1,VNR2,VNR3,VNSFX,VNODOC,VNODCT,VNOSFX,VNPKCO,VNOKCO,VNPDCT,VNAN8,VNCN,VNDKJ,VNDKC,VNASID,VNBRE,VNRCND,VNSUMM,VNPRGE,VNTNN,VNALT1,VNALT2,VNALT3,VNALT4,VNALT5,VNALT6,VNALT7,VNALT8,VNALT9,VNALT0,VNALTT,VNALTU,VNALTV,VNALTW,VNALTX,VNALTZ,VNDLNA,VNCFF1,VNCFF2,VNASM,VNBC,VNVINV,VNIVD,VNWR01,VNPO,VNPSFX,VNDCTO,VNLNID,VNWY,VNWN,VNFNLP,VNOPSQ,VNJBCD,VNJBST,VNHMCU,VNDOI,VNALID,VNALTY,VNDSVJ,VNTORG,VNREG#,VNPYID,VNUSER,VNPID,VNJOBN,VNUPMJ,VNUPMT,VNCRRM,VNACR,VNDGM,VNDGD,VNDGY,VNDG#,VNDICM,VNDICD,VNDICY,VNDIC#,VNDSYM,VNDSYD,VNDSYY,VNDSY#,VNDKM,VNDKD,VNDKY,VNDK#,VNDSVM,VNDSVD,VNDSVY,VNDSV#,VNHDGM,VNHDGD,VNHDGY,VNHDG#,VNDKCM,VNDKCD,VNDKCY,VNDKC#,VNIVDM,VNIVDD,VNIVDY,VNIVD#," & _
            "VNABR1 , VNABR2, VNABR3, VNABR4, VNABT1, VNABT2, VNABT3, VNABT4, VNITM, VNPM01, VNPM02, VNPM03, VNPM04, VNPM05, VNPM06, VNPM07, VNPM08, VNPM09, VNPM10, VNBCRC, VNEXR1, VNTXA1, VNTXITM, VNACTB, VNSTAM, VNCTAM, VNAG, VNAGF, VNTKTX, VNDLNID, VNCKNU)" & _
                  " VALUES (" & _
                  "'AMAZING','','0','1','','" & linea & "','','','0','B','0','0','A','V','" & nxtEDBT & "','','0','','','0','" & fechaProceso & "','0','','','" & nxtICU & "','V','0','0','0','" & empresa & "','" & cuenta & "." & subcuenta & "." & subcuentaAuxiliar & "','2','" & idCuentaContableAux & "','','','','" & ficha & "','A','AA','0','20','0','','" & moneda & "','0','0','0','" & cantidad & "','0','','','','" & Left(nombre, 30) & "','" & Left(observacion, 30) & "','','','','','" & id & "','','','','','','" & ficha & "','','0','0','','','','','','','','','','','','','','','','','','','','','','','','','','','','" & factura & "','" & fechaFactura & "','','','','','0','0','0','','0','','','','0','','','" & fechaProceso & "','','0','0','AMAZING','ZP0411Z1','NTHTTP01VE','" & fechaProceso & "','" & hora & "','D'" & _
                  ",'0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','','','','','','','','','0','','','','','','','','','','','','','','0','','0','0','0','0','0','0','')"

            insertInto400(SQL)

            '------------------------------------------------------------------------------------------

            updateIntoSQLServer("update cabecera_reporte set actualizadoJDE='SI' where id=" & lrdSQL("id") & "")

        End While

        lrdSQL.Close()
        connSQL.Close()

    End Sub

    Private Sub SinReembolso(ByVal id As Integer)

        Dim nxtEDBT As Long
        Dim nxtICU As Long
        Dim linea As Long
        Dim empresa As String
        Dim ficha As String
        Dim cantidad As Double
        Dim moneda As String
        Dim idCuentaContable As String
        Dim factura As String
        Dim observacion As String
        Dim nombre As String
        Dim fechaFactura As String
        Dim fechaProceso As String
        Dim hora As String
        Dim cuenta As String
        Dim subcuenta As String
        Dim subcuentaAuxiliar As String
        Dim idCuentaContableAux As String
        Dim factor As Long

        Dim connSQL As New SqlConnection
        Dim cmdSQL As New SqlCommand
        connSQL.ConnectionString = conexionString
        connSQL.Open()

        cmdSQL.Connection = connSQL
        cmdSQL.CommandText = "select cabecera_reporte.fecha,cabecera_reporte.id,anticipo,abs(total-excedente) as monto_gastado,linea,company,auxiliar,moneda,referencia,observacion,cabecera_reporte.nombre,cabecera_reporte.fecha,detalle_reporte.centro_costo,concepto from cabecera_reporte,detalle_reporte,usuario where cabecera_reporte.id=detalle_reporte.id_cabecera and usuario.ficha=cabecera_reporte.ficha and transferir='SI' and actualizadoJDE='NO' and cabecera_reporte.id=" & id & " "
        Dim lrdSQL As SqlDataReader = cmdSQL.ExecuteReader()

        factor = 1000

        While lrdSQL.Read()

            nxtEDBT = obtenerCorrelativoEDBT()
            nxtICU = obtenerCorrelativoICU()

            linea = CDbl(lrdSQL("linea")) * 1000
            empresa = Trim(lrdSQL("company")) '"00300"
            ficha = Trim(lrdSQL("auxiliar")) '"11409"
            cantidad = Math.Round(CDbl(Math.Round(lrdSQL("monto_gastado"), 2)), 2) * 100 ' 20000 200 Bolivares
            moneda = Trim(lrdSQL("moneda"))
            idCuentaContable = "70015137"
            factura = Trim(lrdSQL("referencia"))
            observacion = Left(id & "-" & Trim(lrdSQL("observacion")), 30)
            nombre = Trim(lrdSQL("nombre"))
            fechaFactura = buscarFechaJuliana(CDate(lrdSQL("fecha")))
            fechaProceso = buscarFechaJuliana(CDate(Now))
            hora = Format(CDate(Now), "hhmmss")
            cuenta = Trim(lrdSQL("centro_costo"))
            subcuenta = buscarCuenta(lrdSQL("concepto")) '"6480"
            subcuentaAuxiliar = buscarSubCuenta(lrdSQL("concepto"))  '"0002"


            linea = CDbl(lrdSQL("linea")) * factor
            cantidad = Math.Round(CDbl(lrdSQL("anticipo")), 2) * 100
            cuenta = lrdSQL("centro_costo")
            subcuenta = buscarCuenta(lrdSQL("concepto"))  '"6480"
            subcuentaAuxiliar = buscarSubCuenta(lrdSQL("concepto"))  '"0002"

            idCuentaContableAux = obtenerIdCuenta(cuenta, subcuenta, subcuentaAuxiliar)

            SQL = "INSERT INTO F0911Z1 ( " & _
            "VNEDUS,VNEDTY,VNEDSQ,VNEDTN,VNEDCT,VNEDLN,VNEDTS,VNEDFT,VNEDDT,VNEDER,VNEDDL,VNEDSP,VNEDTC,VNEDTR,VNEDBT,VNEDGL,VNEDAN,VNKCO,VNDCT,VNDOC,VNDGJ,VNJELN,VNEXTL,VNPOST,VNICU,VNICUT,VNDICJ,VNDSYJ,VNTICU,VNCO,VNANI,VNAM,VNAID,VNMCU,VNOBJ,VNSUB,VNSBL,VNSBLT,VNLT,VNPN,VNCTRY,VNFY,VNFQ,VNCRCD,VNCRR,VNHCRR,VNHDGJ,VNAA,VNU,VNUM,VNGLC,VNRE,VNEXA,VNEXR,VNR1,VNR2,VNR3,VNSFX,VNODOC,VNODCT,VNOSFX,VNPKCO,VNOKCO,VNPDCT,VNAN8,VNCN,VNDKJ,VNDKC,VNASID,VNBRE,VNRCND,VNSUMM,VNPRGE,VNTNN,VNALT1,VNALT2,VNALT3,VNALT4,VNALT5,VNALT6,VNALT7,VNALT8,VNALT9,VNALT0,VNALTT,VNALTU,VNALTV,VNALTW,VNALTX,VNALTZ,VNDLNA,VNCFF1,VNCFF2,VNASM,VNBC,VNVINV,VNIVD,VNWR01,VNPO,VNPSFX,VNDCTO,VNLNID,VNWY,VNWN,VNFNLP,VNOPSQ,VNJBCD,VNJBST,VNHMCU,VNDOI,VNALID,VNALTY,VNDSVJ,VNTORG,VNREG#,VNPYID,VNUSER,VNPID,VNJOBN,VNUPMJ,VNUPMT,VNCRRM,VNACR,VNDGM,VNDGD,VNDGY,VNDG#,VNDICM,VNDICD,VNDICY,VNDIC#,VNDSYM,VNDSYD,VNDSYY,VNDSY#,VNDKM,VNDKD,VNDKY,VNDK#,VNDSVM,VNDSVD,VNDSVY,VNDSV#,VNHDGM,VNHDGD,VNHDGY,VNHDG#,VNDKCM,VNDKCD,VNDKCY,VNDKC#,VNIVDM,VNIVDD,VNIVDY,VNIVD#," & _
            "VNABR1 , VNABR2, VNABR3, VNABR4, VNABT1, VNABT2, VNABT3, VNABT4, VNITM, VNPM01, VNPM02, VNPM03, VNPM04, VNPM05, VNPM06, VNPM07, VNPM08, VNPM09, VNPM10, VNBCRC, VNEXR1, VNTXA1, VNTXITM, VNACTB, VNSTAM, VNCTAM, VNAG, VNAGF, VNTKTX, VNDLNID, VNCKNU)" & _
                  " VALUES (" & _
                  "'AMAZING','','0','1','','" & linea & "','','','0','B','0','0','A','J','" & nxtEDBT & "','','0','','UL','0','" & fechaProceso & "','0','','','" & nxtICU & "','G','0','0','0','" & empresa & "','" & cuenta & "." & subcuenta & "." & subcuentaAuxiliar & "','2','" & idCuentaContableAux & "','','','','','','AA','0','20','0','','" & moneda & "','0','0','0','" & cantidad & "','0','','','','" & Left(nombre, 30) & "','" & Left(observacion, 30) & "','','','','','" & id & "','','','','','','" & ficha & "','','0','0','','','','','','','','','','','','','','','','','','','','','','','','','','','','" & factura & "','" & fechaFactura & "','','','','','0','0','0','','0','','','','0','','','" & fechaProceso & "','','0','0','AMAZING','ZP0411Z1','NTHTTP01VE','" & fechaProceso & "','" & hora & "','D'" & _
                  ",'0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','','','','','','','','','0','','','','','','','','','','','','','','0','','0','0','0','0','0','0','')"

           

            insertInto400(SQL)

            '------------------------------------------------------------------------------------------

            factor = factor + 1000
            linea = CDbl(lrdSQL("linea")) * factor
            cantidad = Math.Round(CDbl(lrdSQL("anticipo")), 2) * 100 * -1  ' ANTICIPO
            cuenta = "3009"
            subcuenta = "1260"
            subcuentaAuxiliar = "1003"


            idCuentaContableAux = obtenerIdCuenta(cuenta, subcuenta, subcuentaAuxiliar)

            SQL = "INSERT INTO F0911Z1 ( " & _
            "VNEDUS,VNEDTY,VNEDSQ,VNEDTN,VNEDCT,VNEDLN,VNEDTS,VNEDFT,VNEDDT,VNEDER,VNEDDL,VNEDSP,VNEDTC,VNEDTR,VNEDBT,VNEDGL,VNEDAN,VNKCO,VNDCT,VNDOC,VNDGJ,VNJELN,VNEXTL,VNPOST,VNICU,VNICUT,VNDICJ,VNDSYJ,VNTICU,VNCO,VNANI,VNAM,VNAID,VNMCU,VNOBJ,VNSUB,VNSBL,VNSBLT,VNLT,VNPN,VNCTRY,VNFY,VNFQ,VNCRCD,VNCRR,VNHCRR,VNHDGJ,VNAA,VNU,VNUM,VNGLC,VNRE,VNEXA,VNEXR,VNR1,VNR2,VNR3,VNSFX,VNODOC,VNODCT,VNOSFX,VNPKCO,VNOKCO,VNPDCT,VNAN8,VNCN,VNDKJ,VNDKC,VNASID,VNBRE,VNRCND,VNSUMM,VNPRGE,VNTNN,VNALT1,VNALT2,VNALT3,VNALT4,VNALT5,VNALT6,VNALT7,VNALT8,VNALT9,VNALT0,VNALTT,VNALTU,VNALTV,VNALTW,VNALTX,VNALTZ,VNDLNA,VNCFF1,VNCFF2,VNASM,VNBC,VNVINV,VNIVD,VNWR01,VNPO,VNPSFX,VNDCTO,VNLNID,VNWY,VNWN,VNFNLP,VNOPSQ,VNJBCD,VNJBST,VNHMCU,VNDOI,VNALID,VNALTY,VNDSVJ,VNTORG,VNREG#,VNPYID,VNUSER,VNPID,VNJOBN,VNUPMJ,VNUPMT,VNCRRM,VNACR,VNDGM,VNDGD,VNDGY,VNDG#,VNDICM,VNDICD,VNDICY,VNDIC#,VNDSYM,VNDSYD,VNDSYY,VNDSY#,VNDKM,VNDKD,VNDKY,VNDK#,VNDSVM,VNDSVD,VNDSVY,VNDSV#,VNHDGM,VNHDGD,VNHDGY,VNHDG#,VNDKCM,VNDKCD,VNDKCY,VNDKC#,VNIVDM,VNIVDD,VNIVDY,VNIVD#," & _
            "VNABR1 , VNABR2, VNABR3, VNABR4, VNABT1, VNABT2, VNABT3, VNABT4, VNITM, VNPM01, VNPM02, VNPM03, VNPM04, VNPM05, VNPM06, VNPM07, VNPM08, VNPM09, VNPM10, VNBCRC, VNEXR1, VNTXA1, VNTXITM, VNACTB, VNSTAM, VNCTAM, VNAG, VNAGF, VNTKTX, VNDLNID, VNCKNU)" & _
                  " VALUES (" & _
                  "'AMAZING','','0','1','','" & linea & "','','','0','B','0','0','A','J','" & nxtEDBT & "','','0','','UL','0','" & fechaProceso & "','0','','','" & nxtICU & "','G','0','0','0','" & empresa & "','" & cuenta & "." & subcuenta & "." & subcuentaAuxiliar & "','2','" & idCuentaContableAux & "','','','','" & ficha & "','A','AA','0','20','0','','" & moneda & "','0','0','0','" & cantidad & "','0','','','','" & Left(nombre, 30) & "','" & Left(observacion, 30) & "','','','','','" & id & "','','','','','','" & ficha & "','','0','0','','','','','','','','','','','','','','','','','','','','','','','','','','','','" & factura & "','" & fechaFactura & "','','','','','0','0','0','','0','','','','0','','','" & fechaProceso & "','','0','0','AMAZING','ZP0411Z1','NTHTTP01VE','" & fechaProceso & "','" & hora & "','D'" & _
                  ",'0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','','','','','','','','','0','','','','','','','','','','','','','','0','','0','0','0','0','0','0','')"

           insertInto400(SQL)

            '------------------------------------------------------------------------------------------

            updateIntoSQLServer("update cabecera_reporte set actualizadoJDE='SI' where id=" & lrdSQL("id") & "")

        End While

        lrdSQL.Close()
        connSQL.Close()

    End Sub

   
    Private Sub insertInto400(ByVal sql As String)

        Dim cnn400 As New Odbc.OdbcConnection(cadenaAS400_DTA)
        cnn400.Open()
        Cmd400.ActiveConnection = Conn400
        Cmd400.CommandText = SQL
        Cmd400.Execute()
        cnn400.Close()

    End Sub

    Private Sub updateIntoSQLServer(ByVal sql As String)

        Dim connSQL As New SqlConnection
        Dim cmdSQL As New SqlCommand
        connSQL.ConnectionString = conexionString
        connSQL.Open()

        cmdSQL.Connection = connSQL
        cmdSQL.CommandText = sql
        cmdSQL.ExecuteNonQuery()
        connSQL.Close()

    End Sub


    Private Function obtenerCorrelativoEDBT() As Long

        Dim correlativo As Long
        Dim cnn400 As New Odbc.OdbcConnection(cadenaAS400_DTA)
        Dim rs400 As New Odbc.OdbcCommand("SELECT NNN006 FROM F0002 WHERE NNSY='00' ", cnn400)
        Dim reader400 As Odbc.OdbcDataReader

        cnn400.Open()
        reader400 = rs400.ExecuteReader

        While reader400.Read()

            correlativo = reader400("NNN006")

            Cmd400.ActiveConnection = Conn400
            Cmd400.CommandText = "UPDATE F0002 SET NNN006=NNN006+1 WHERE NNSY='00' "
            Cmd400.Execute()

        End While

        reader400.Close()
        cnn400.Close()

        obtenerCorrelativoEDBT = correlativo

    End Function

    Private Function obtenerCorrelativoICU() As Long

        Dim correlativo As Long
        Dim cnn400 As New Odbc.OdbcConnection(cadenaAS400_DTA)
        Dim rs400 As New Odbc.OdbcCommand("SELECT NNN001 FROM F0002 WHERE NNSY='00' ", cnn400)
        Dim reader400 As Odbc.OdbcDataReader

        cnn400.Open()
        reader400 = rs400.ExecuteReader

        While reader400.Read()

            correlativo = reader400("NNN001")

            Cmd400.ActiveConnection = Conn400
            Cmd400.CommandText = "UPDATE F0002 SET NNN001=NNN001+1 WHERE NNSY='00' "
            Cmd400.Execute()

        End While

        reader400.Close()
        cnn400.Close()

        obtenerCorrelativoICU = correlativo

    End Function


    Private Function obtenerIdCuenta(ByVal MCU As String, ByVal OBJ As String, ByVal SUB1 As String) As String

        Dim id As String

        Dim cnn400 As New Odbc.OdbcConnection(cadenaAS400_DTA)
        Dim rs400 As New Odbc.OdbcCommand("SELECT GMAID FROM F0901 WHERE GMMCU='" & MCU & "' AND GMOBJ='" & Trim(OBJ) & "  ' AND GMSUB='" & Trim(SUB1) & "    '", cnn400)
        Dim reader400 As Odbc.OdbcDataReader

        id = 0

        If Len(Trim(MCU)) <> 12 Then
            MCU = "        " & Trim(MCU)
        End If

        cnn400.Open()
        reader400 = rs400.ExecuteReader

        While reader400.Read()
            id = reader400("GMAID")
        End While

        reader400.Close()
        cnn400.Close()

        obtenerIdCuenta = id
    End Function

    Private Function buscarFechaJuliana(ByVal fecha As Date) As String

        Dim connSQL As New SqlConnection
        Dim cmdSQL As New SqlCommand
        connSQL.ConnectionString = conexionString
        connSQL.Open()


        Dim valor As String
        Dim bisiesto As String

        If IsAñoBisiesto(Year(CDate(fecha))) Then
            bisiesto = "S"
        Else
            bisiesto = "N"
        End If

        valor = ""

        cmdSQL.Connection = connSQL
        cmdSQL.CommandText = "SELECT DIA FROM CONVERSION_FECHA WHERE ESBISIESTO='" & bisiesto & "' AND  substring(fecha,5,4) = '" & Right("00" & Month(CDate(fecha)), 2) & Right("00" & Day(CDate(fecha)), 2) & "'  "
        Dim lrdSQL As SqlDataReader = cmdSQL.ExecuteReader()

        While lrdSQL.Read()
            valor = "1" & Right(Year(CDate(fecha)), 2) & lrdSQL("DIA")
        End While

        lrdSQL.Close()
        connSQL.Close()


        buscarFechaJuliana = valor
    End Function

    Function IsAñoBisiesto(ByVal YYYY As Integer) As Boolean
        IsAñoBisiesto = YYYY Mod 4 = 0 _
                    And (YYYY Mod 100 <> 0 Or YYYY Mod 400 = 0)
    End Function

    Private Function buscarCuenta(ByVal concepto As String) As String

        Dim connSQL As New SqlConnection
        Dim cmdSQL As New SqlCommand
        connSQL.ConnectionString = conexionString
        connSQL.Open()

        Dim valor As String
        valor = ""

        cmdSQL.Connection = connSQL
        cmdSQL.CommandText = "SELECT  valorstr2   From [dusa_reporte_gasto].[dbo].[udc]   where sistema='TABLAS' and subsistema='CONCEPTO' and valorstr1='" & concepto & "'  "
        Dim lrdSQL As SqlDataReader = cmdSQL.ExecuteReader()

        While lrdSQL.Read()
            valor = lrdSQL("valorstr2")
        End While

        lrdSQL.Close()
        connSQL.Close()

        buscarCuenta = valor

    End Function


    Private Function buscarSubCuenta(ByVal concepto As String) As String


        Dim connSQL As New SqlConnection
        Dim cmdSQL As New SqlCommand
        connSQL.ConnectionString = conexionString
        connSQL.Open()

        Dim valor As String
        valor = ""

        cmdSQL.Connection = connSQL
        cmdSQL.CommandText = "SELECT  valorstr3   From [dusa_reporte_gasto].[dbo].[udc]   where sistema='TABLAS' and subsistema='CONCEPTO' and valorstr1='" & concepto & "'  "
        Dim lrdSQL As SqlDataReader = cmdSQL.ExecuteReader()

        While lrdSQL.Read()
            valor = lrdSQL("valorstr3")
        End While

        lrdSQL.Close()
        connSQL.Close()

        buscarSubCuenta = valor

    End Function

    Private Sub cargar_parametros()

        Dim host As String
        Dim database As String
        Dim user As String
        Dim password As String

        Dim conexion As New SqlConnection
        Dim comando As New SqlClient.SqlCommand

        Dim diccionario As New Dictionary(Of String, String)
        Dim xmldoc As New XmlDataDocument()
        Dim file_log_path As String

        Try
            file_log_path = Replace(System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().GetName().CodeBase), "file:\", "")
            If System.IO.File.Exists(file_log_path & "\log.log") Then
            Else
                Dim fs1 As FileStream = File.Create(file_log_path & "\log.log")
                fs1.Close()
            End If

            Try
                logger = New StreamWriter(Replace(System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().GetName().CodeBase), "file:\", "") & "\log.log", True)
            Catch ex As Exception

            End Try


            Dim fs As New FileStream(Replace(System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().GetName().CodeBase), "file:\", "") & "\integrador.xml", FileMode.Open, FileAccess.Read)
            xmldoc = New XmlDataDocument()
            xmldoc.Load(fs)
            diccionario = obtenerNodosHijosDePadre("parametros", xmldoc)
            host = diccionario.Item("host")
            database = diccionario.Item("database")
            user = diccionario.Item("user")
            password = diccionario.Item("password")

            conexionString = "Data Source=" & host & ";Database=" & database & ";User ID=" & user & ";Password=" & password & ";"

        Catch oe As Exception
            escribirLog(oe.StackTrace.ToString & "-" & oe.Message.ToString, "(MATMAS) ")
        Finally

            logger.Close()
        End Try

    End Sub


    Public Sub escribirLog(ByVal mensaje As String, ByVal proceso As String)

        Dim time As DateTime = DateTime.Now
        Dim format As String = "dd/MM/yyyy HH:mm "

        lineaLogger = proceso & time.ToString(format) & ":" & mensaje & vbNewLine
        logger.WriteLine(lineaLogger)
        logger.Flush()

    End Sub

    Public Function obtenerNodosHijosDePadre(ByVal nombreNodoPadre As String, ByVal xmldoc As XmlDataDocument) As Dictionary(Of String, String)
        Dim diccionario As New Dictionary(Of String, String)
        Dim nodoPadre As XmlNodeList
        Dim i As Integer
        Dim h As Integer
        nodoPadre = xmldoc.GetElementsByTagName(nombreNodoPadre)
        For i = 0 To nodoPadre.Count - 1
            For h = 0 To nodoPadre(i).ChildNodes.Count - 1
                If Not diccionario.ContainsKey(nodoPadre(i).ChildNodes.Item(h).Name.Trim()) Then
                    diccionario.Add(nodoPadre(i).ChildNodes.Item(h).Name.Trim(), nodoPadre(i).ChildNodes.Item(h).InnerText.Trim())
                End If
            Next
        Next
        Return diccionario
    End Function
   
End Module
