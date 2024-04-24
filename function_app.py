#packages for Azure Function App
import azure.functions as func
import logging
#packages for MSCI 
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy as db
from suds.client import Client
from suds import WebFault
from urllib import parse
import time
import sys
import logging
from bs4 import BeautifulSoup as bs
import sys
from xml.etree import ElementTree as ET
import datetime as dt

app = func.FunctionApp()


@app.route(route="fcMSCiPushPortfolio", auth_level=func.AuthLevel.ANONYMOUS)
def fcMSCiPushPortfolio(req: func.HttpRequest) -> func.HttpResponse:

    logging.info('Python HTTP trigger function [fcMSCiPushPortfolio] received a request.')
    
    try:
        msci = extMSCiTasks()
        msci.sendPortToMsci()
    except Exception as e:
        logging.exception("fcMSCiPushPortfolio function failed with exception: %s", e)
    
    logging.info('Python HTTP trigger function [fcMSCiPushPortfolio] processed a request.')
    return func.HttpResponse(f"Success running AMF portfolio push to MSCi [using AmfAzurePythonApps..fcMSCiPushPortfolio]", status_code=200)
    

@app.route(route="fcMSCiGetData", auth_level=func.AuthLevel.ANONYMOUS)
def fcMSCiGetData(req: func.HttpRequest) -> func.HttpResponse:

    logging.info('Python HTTP trigger function [fcMSCiGetData] received a request.')
    
    try:
        msci = extMSCiTasks()
        msci.getResutsFromMsci()
    except Exception as e:
        logging.exception("fcMSCiPushPortfolio function failed with exception: %s", e)

    logging.info('Python HTTP trigger function [fcMSCiGetData] processed a request.')
    return func.HttpResponse(f"Success pulling AMF risk report data from MSCi [using AmfAzurePythonApps..fcMSCiGetData]", status_code=200)
    

@app.route(route="fcGetLatestMSCiData", auth_level=func.AuthLevel.ANONYMOUS)
def fcGetLatestMSCiData(req: func.HttpRequest) -> func.HttpResponse:

    logging.info('Python HTTP trigger function [fcGetLatestMSCiData] received a request.')
    html_msci = "<emf>No Results from data request<emp/>"

    try:
        msci = extMSCiTasks()
        html_msci = msci.getMSCiLatestResults()
    except Exception as e:
        logging.exception("fcGetLatestMSCiData function failed with exception: %s", e)

    logging.info('Python HTTP trigger function [fcGetLatestMSCiData] processed a request.')
    return func.HttpResponse(html_msci, status_code=200)


@app.route(route="fcGetEstUniverseRisk", auth_level=func.AuthLevel.ANONYMOUS)
def fcGetEstUniverseRisk(req: func.HttpRequest) -> func.HttpResponse:

    logging.info('Python HTTP trigger function [fcGetEstUniverseRisk] received a request.')

    try:
        msci = extMSCiTasks()
        msci.getEstUniverseResutsFromMsci()
    except Exception as e:
        logging.exception("fcGetLatestMSCiData function failed with exception: %s", e)

    logging.info('Python HTTP trigger function [fcGetEstUniverseRisk] processed a request.')
    return func.HttpResponse(f"Success pulling AMF risk report data from MSCi [using AmfAzurePythonApps..fcGetEstUniverseRisk]", status_code=200)


class extMSCiTasks():
    def __init__(self):
        self.name = "extMSCiTasks"

    @staticmethod
    def sendPortToMsci():        
        logging.info("Call to send AMF portfolio to MSCI via API") 

        def ClearAsOfMSCiData(dtAsOfDate):  
            conn_str = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:allostery-dev.database.windows.net,1433;Database=Operations;UID=svcOperations;PWD=Man@ger22!;MultipleActiveResultSets=False;Trusted_Connection=no;Auto_Commit=yes' 
            prms = parse.quote_plus(conn_str)
            eng = db.create_engine("mssql+pyodbc:///?odbc_connect=%s" % prms)
            
            query = """
            DECLARE @out int;
            EXEC [dbo].[p_ClearMSCiBetas] @AsOfDate = :p1;
            SELECT @out AS the_output;
            """
            params = dict(p1= dtAsOfDate)

            with eng.connect() as conn:
                result = conn.execute(db.text(query), params).scalar()
                conn.commit()
                conn.close()

        conn_str = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:allostery-dev.database.windows.net,1433;Database=Operations;UID=svcOperations;PWD=Man@ger22!;MultipleActiveResultSets=False;Trusted_Connection=no;Auto_Commit=yes' 
        params = parse.quote_plus(conn_str)
        engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
        connection = engine.connect()

        dn = pd.read_sql("EXEC [dbo].[p_GetAMFNavValues]", engine)
        amfNav = dn.NavValue.astype('double').values[0]
        analysisDt = dn.AsOfDate.values[0]
        df = pd.read_sql("EXEC [dbo].[p_GetSimplePort] @PortDate ='" + analysisDt.strftime(r'%m/%d/%y') + "'", engine)

        ClearAsOfMSCiData(analysisDt)
        connection.close()

        # Open connection to MSCI
        usr = "ITAdmin"
        pwd = "7GRYPnZQaVWnnLRzU9yd"
        cid = "rkvi74supd"

        wsdl = "https://www.barraone.com/axis2/services/BDTService?wsdl"
        client = Client(wsdl, location=wsdl, timeout=5000)

        logging.info('Created MSCI API connection client')

        # create a portfolio object
        myPort = client.factory.create('Portfolio')
        myPort._PortfolioName = 'AMF-Main-Daily'

        myPort._Owner = usr
        myPort._PortfolioImportType = client.factory.create("PortfolioImportType").BY_HOLDINGS
        myPort._PortfolioValue = amfNav

        myPositions = client.factory.create('Positions')
        myPosList = []

        df = df.reset_index()  
        for index, row in df.iterrows():
            myPos = client.factory.create('Position')
            myPos._Holdings = row['Quantity']
            myPos._Currency = 'USD'
            myMIDList = []
            myMid = client.factory.create('MID')
            myMid._ID = row['TickerName'].strip()
            myMIDList.append(myMid)
            myPos.MID = myMIDList
            myPosList.append(myPos)
            
        analysisDt = row['PortDate']
        myPort._EffectiveStartDate = analysisDt

        myPositions.Position = myPosList
        myPort.Positions = myPositions

        # SENDING PORTFOLIO TO THE MSCI API
        logging.info("Sending portfolio to MSCi API ...") 
        print('Sending portfolio to MSCi API ...')
        jobID = client.service.SubmitImportJob(User=usr, Client=cid, Password=pwd, JobName="MSCI test", Portfolio=[myPort])
        logging.info("Job Id for running batch: ", jobID) 
        print("Job Id for running batch: ", jobID)
        time.sleep(5)  # wait time of 5 secs is required before GetImportJobStatus

        sleepTime = 30
        while sleepTime > 0:
            try:
                sleepTime = client.service.GetImportJobStatus(usr, cid, pwd, jobID)
            except WebFault as detail:
                logging.exception(detail)
                print(detail)
            except:
                logging.exception("sendPortToMsci function failed with an unexpected error: %s", sys.exc_info()[0])
                print("sendPortToMsci function failed with an unexpected error: %s", sys.exc_info()[0])
                raise
            #print(sleepTime)
            if sleepTime > 0:
                time.sleep(5)

        logResponse = client.service.GetImportJobLog(usr, cid, pwd, jobID)
        
        logging.info("sendPortToMsci function call to MSCi API job name: ", logResponse._JobName) 
        print("sendPortToMsci function call to MSCi  API job name: ", logResponse._JobName)

        if sleepTime == 0:
           
            logging.info("Job successful. Getting import log...")
            print('Job successful. Getting import log...')
            logging.info("Date, Name, Owner, Total, Rejects, Blanks, Duplicates, Deleted, Msg")
            print("Date, Name, Owner, Total, Rejects, Blanks, Duplicates, Deleted, Msg")

            for ejr in logResponse.LogGroups.ImportLogGroup:
                logging.info(ejr._EffectiveDate, " ", ejr._Name, " ", ejr._Owner, " ", ejr._Total, " ", ejr._Rejected, " ", ejr._Blank, " ", ejr._Duplicate, " ", ejr._Deleted, " ", ejr._ResultMsg)
                print(ejr._EffectiveDate, " ", ejr._Name, " ", ejr._Owner, " ", ejr._Total, " ", ejr._Rejected, " ", ejr._Blank, " ", ejr._Duplicate, " ", ejr._Deleted, " ", ejr._ResultMsg)
                for grp in ejr.Details.ImportLogDetail:
                    if grp._ResultMsg.startswith('Risk model'):
                        logging.info(">> *** ", grp._ResultMsg, " ", grp._ResultCode)
                        print(">> *** ", grp._ResultMsg, " ", grp._ResultCode)
                    elif grp._ResultMsg.startswith('Success clear'):
                        logging.info(">> ", grp._ResultMsg, " ", grp._Detail1, " ", grp._ResultCode)
                        print(">> ", grp._ResultMsg, " ", grp._Detail1, " ", grp._ResultCode)
                    else:
                        logging.info(">> ", grp._ResultMsg, " ", grp._Detail1)
                        print(">> ", grp._ResultMsg, " ", grp._Detail1)

        else:
            logging.info("Job failed. Please see log file for error details.")
            print("Job failed. Please see log file for error details.")
            logging.info("Date, Name, Owner")
            print("Date, Name, Owner")
            for ejr in logResponse.LogGroups.ImportLogGroup:
                logging.info(ejr._EffectiveDate, ", ", ejr._Name, ", ", ejr._Owner)
                print(ejr._EffectiveDate, ", ", ejr._Name, ", ", ejr._Owner)                
                for grp in ejr.Details.ImportLogDetail:
                    logging.info(">> ", grp._ResultMsg, " ", grp._Detail1)
                    print(">> ", grp._ResultMsg, " ", grp._Detail1)                    

    @staticmethod
    def getResutsFromMsci():        
        logging.info("Call to get AMF data from MSCI via API")       

        def LoadDataToDatabase(sTicker, sSecName, fQuantity, fPrice, fMktVal, fWeight, fMktCorr, fBmkCorr):
            conn_str = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:allostery-dev.database.windows.net,1433;Database=Operations;UID=svcOperations;PWD=Man@ger22!;MultipleActiveResultSets=False;Trusted_Connection=no;Auto_Commit=yes' 
            prms = parse.quote_plus(conn_str)
            eng = db.create_engine("mssql+pyodbc:///?odbc_connect=%s" % prms)

            query = """
            DECLARE @out int;
            EXEC [dbo].[p_SetCorrValuesMSCi] @AsOfDate = :p1, @PortfolioName = :p2, @Ticker = :p3, @SecName = :p4, @Quantity = :p5, @Price = :p6, @MktVal = :p7, @WeightMod = :p8, @MktCorr = :p9, @BmkCorr = :p10;
            SELECT @out AS the_output;
            """
            params = dict(p1= analysisDt.strftime(r'%m/%d/%y'), p2=portfolio, p3=sTicker, p4=sSecName, p5=fQuantity, p6=fPrice, p7=fMktVal, p8=fWeight, p9=fMktCorr, p10=fBmkCorr)
            with eng.connect() as conn:
                result = conn.execute(db.text(query), params).scalar()
                conn.commit()
                conn.close()

        # Runs a report based on the reportID obtained from the supported reports
        def RetrieveReportsSample(reportID, custTemplateName, custTemplateOwner, date, por, porOwner, analysisSetting, analysisSettingOwner, tsName, tsOwner):
            print('Retrieving report:', reportID, custTemplateName, custTemplateOwner)
            result = ''

            if reportID == 'CUSTOM':
                repTmp = client.factory.create('ReportTemplate')
                repTmp._TemplateName = custTemplateName
                repTmp._TemplateOwner = custTemplateOwner

                port = client.factory.create('InputPortfolio')
                port._Name = por
                port._Owner = porOwner

                porList = client.factory.create('Portfolios')
                porList.Portfolio = port

                aSetting = client.factory.create('InputAnalysisSettings')
                aSetting._Name = analysisSetting
                aSetting._Owner = analysisSettingOwner

                repDef = client.factory.create('RiskReportsDefinition')
                repDef.AnalysisDate = date
                repDef.Portfolios = porList
                repDef.AnalysisSettings = aSetting

                result = client.service.RetrieveReports(usr, cid, pwd, None, None, repDef, None, repTmp)

            if reportID == 'FACTOR_EXPOSURE' or reportID == 'RISK_ATTRIBUTION':
                repParams = client.factory.create('ReportParametersDef')
                repParams._ReportId = reportID

                port = client.factory.create('InputPortfolio')
                port._Name = por
                port._Owner = porOwner

                porList = client.factory.create('Portfolios')
                porList.Portfolio = port

                aSetting = client.factory.create('InputAnalysisSettings')
                aSetting._Name = analysisSetting
                aSetting._Owner = analysisSettingOwner

                repDef = client.factory.create('RiskReportsDefinition')
                repDef.AnalysisDate = date
                repDef.Portfolios = porList
                repDef.AnalysisSettings = aSetting

                result = client.service.RetrieveReports(usr, cid, pwd, None, None, repDef, repParams)

            if reportID == 'BPM_PA_TOTAL_RETURN_REPORT' or reportID == 'BPM_PA_TS_ATTRIBUTION_REPORT':
                port = client.factory.create('InputPortfolio')
                port._Name = por
                port._Owner = porOwner

                porList = client.factory.create('Portfolios')
                porList.Portfolio = port

                aSetting = client.factory.create('InputAnalysisSettings')
                aSetting._Name = analysisSetting
                aSetting._Owner = analysisSettingOwner

                ts = client.factory.create('InputTimeSeriesSettings')
                ts._Name = tsName
                ts._Owner = tsOwner

                repDef = client.factory.create('ReturnAttribution')
                repDef.TimeSeriesSettings = ts
                repDef.Portfolios = porList
                repDef.AnalysisSettings = aSetting

                paJob = client.service.SubmitReportJob(usr, cid, pwd, repDef)
                # print(paJob)
                taskId = paJob._TaskId
                jobId = paJob.JobId[0]

                print('Waiting for job...')
                print('TaskID: ', taskId)
                print('JobID:  ', jobId)

                time.sleep(3)

                jobStat = 30
                while jobStat == 30:
                    try:
                        jobStat = client.service.GetReportJobStatus(usr, cid, pwd, jobId)
                    except WebFault as detail:
                        print(detail)
                    except:
                        print("Unexpected error:", sys.exc_info()[0])
                        raise
                    if jobStat > 0:
                        time.sleep(jobStat / 2)

                if jobStat == -1:
                    print('Error!!')
                else:
                    repParams = client.factory.create('ReportParametersDef')
                    repParams._ReportId = reportID

                    result = client.service.RetrieveReports(usr, cid, pwd, taskId, None, None, repParams)

            if reportID == 'CUSTOM_TS':
                port = client.factory.create('InputPortfolio')
                port._Name = por
                port._Owner = porOwner

                porList = client.factory.create('Portfolios')
                porList.Portfolio = port

                aSetting = client.factory.create('InputAnalysisSettings')
                aSetting._Name = analysisSetting
                aSetting._Owner = analysisSettingOwner

                ts = client.factory.create('InputTimeSeriesSettings')
                ts._Name = tsName
                ts._Owner = tsOwner

                repDef = client.factory.create('ReturnAttribution')
                repDef.TimeSeriesSettings = ts
                repDef.Portfolios = porList
                repDef.AnalysisSettings = aSetting

                paJob = client.service.SubmitReportJob(usr, cid, pwd, repDef)
                taskId = paJob._TaskId
                jobId = paJob.JobId[0]

                print('Waiting for job...')
                print('TaskID: ', taskId)
                print('JobID:  ', jobId)

                time.sleep(3)

                jobStat = 30
                while jobStat == 30:
                    try:
                        jobStat = client.service.GetReportJobStatus(usr, cid, pwd, jobId)
                    except WebFault as detail:
                        print(detail)
                    except:
                        print("Unexpected error:", sys.exc_info()[0])
                        raise
                    if jobStat > 0:
                        time.sleep(jobStat / 2)

                if jobStat == -1:
                    print('Error! The job has failed. To view the log please rerun from the BPM UI.')
                    sys.exit()
                else:
                    repTmp = client.factory.create('ReportTemplate')
                    repTmp._TemplateName = custTemplateName
                    repTmp._TemplateOwner = custTemplateOwner

                    repParams = client.factory.create('RetrieveReportsInputParams')
                    repParams.TaskId = taskId
                    repParams.ReportTemplate = repTmp

                    result = client.service.RetrieveTemplateReports(usr, cid, pwd, repParams)
                    result = result.Response
                    print(result)

            for item in result:
                for data in item:
                    if data != "ExportJobReport":
                        df = data[0].ReportBody.ReportBodyGroup[0].ReportBodyRow
                        dx = data[1].ReportBody.ReportBodyGroup[0].ReportBodyRow

            for pos in dx:
                sTicker = pos.CellData[1]._Value
                sSecName = pos.CellData[2]._Value
                fQuantity = pos.CellData[3]._Value
                fPrice = pos.CellData[4]._Value
                fMktVal = pos.CellData[5]._Value
                fWeight = pos.CellData[6]._Value
                fMktCorr = pos.CellData[7]._Value
                fBmkCorr = pos.CellData[8]._Value        

                if sSecName != "":
                    if fPrice != "N/A":
                        LoadDataToDatabase(sTicker,  sSecName, fQuantity, fPrice, fMktVal, fWeight, fMktCorr, fBmkCorr)
                        print(str(sTicker) + " " + str(sSecName) + " " + str(fQuantity)+ " " + str(fPrice)+ " " + str(fMktVal)+ " " + str(fWeight) + " " + str(fMktCorr) + " " + str(fBmkCorr))

            print("Response end, code complete.")

        file = "https://www.barraone.com/axis2/services/BDTService?wsdl"
        client = Client(file, location=file, timeout=5000, retxml=False)

        # Open connection to MSCI
        usr = "ITAdmin"
        pwd = "7GRYPnZQaVWnnLRzU9yd"
        cid = "rkvi74supd"

        # settings
        conn_str = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:allostery-dev.database.windows.net,1433;Database=Operations;UID=svcOperations;PWD=Man@ger22!;MultipleActiveResultSets=False;Trusted_Connection=no;Auto_Commit=yes' 
        params = parse.quote_plus(conn_str)
        engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
        connection = engine.connect()

        dn = pd.read_sql("EXEC [dbo].[p_GetAMFNavValues]", engine)
        analysisDt = dn.AsOfDate.values[0]
        connection.close()

        #analysisDt = dt.date(2024,1,5)
        portfolio = 'AMF-Main-Daily'
        portfolioOwner = usr
        analysisSetting = "EFMGEMLTS-Benchmark"
        analysisSettingOwner = usr
        timeSeriesName = "tsSettings"
        timeSeriesOwner = usr
        customTemplate = "AMF-PositionReportBetas"
        customTemplateOwner = usr

        try:    
            # USE THIS ONE    
            RetrieveReportsSample("CUSTOM", customTemplate, customTemplateOwner, analysisDt, portfolio, portfolioOwner, analysisSetting, analysisSettingOwner, None, None)
            
        except WebFault as detail:
            logging.exception("getResutsFromMsci WebFault exception: %s", detail)
            print(detail)
        except Exception as e:
            logging.exception("getResutsFromMsci function failed with exception: %s", sys.exc_info()[0])

        except:
            logging.exception("getResutsFromMsci function failed with and unexpected error: %s", sys.exc_info()[0])
            print("getResutsFromMsci function failed with and unexpected error:", sys.exc_info()[0])
            raise

        print("All ok, script completed.")

    @staticmethod
    def getMSCiLatestResults():
        logging.info("Call to get MSCI HTML table from database.")  

        conn_str = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:allostery-dev.database.windows.net,1433;Database=Operations;UID=svcOperations;PWD=Man@ger22!;MultipleActiveResultSets=False;Trusted_Connection=no;Auto_Commit=yes' 
        params = parse.quote_plus(conn_str)
        engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
        connection = engine.connect()

        dn = pd.read_sql("EXEC [dbo].[p_GetMSCiBetas]", engine)
        connection.close()

        table = "<table border=1 ><tr><th>Ticker</th><th>Quantity</th><th>Bmk Beta</th></tr>"
        tr = ""
        td_0 = ""
        td_1 = ""
        td_2 = ""

        for index, row in dn.iterrows():
          bbYellowKey = row['BbgYellowKey']
          quant = row['Quantity']
          bmkcorr = row['BmkCorr']

          td_0 = "{}<hr>".format(bbYellowKey)
          td_1 = "{}<hr>".format(f'{quant:,.0f}')
          td_2 = "{}<hr>".format(f'{bmkcorr:,.2f}')
          tr += "<tr><td valign='top' align='left'>{}</td><td valign='top' align='right'>{}</td><td valign='top' align='right'>{}</td></tr>".format(td_0, td_1, td_2)
        end = "</table>"
        table = table + tr + end
                
        return table

    @staticmethod
    def getEstUniverseResutsFromMsci():        
        logging.info("Call to get AMF data from MSCI via API")       

        def LoadDataToDatabase(sTicker, sSecName, fQuantity, fPrice, fMktVal, fWeight, fMktCorr, fBmkCorr):
            conn_str = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:allostery-dev.database.windows.net,1433;Database=Operations;UID=svcOperations;PWD=Man@ger22!;MultipleActiveResultSets=False;Trusted_Connection=no;Auto_Commit=yes' 
            prms = parse.quote_plus(conn_str)
            eng = db.create_engine("mssql+pyodbc:///?odbc_connect=%s" % prms)

            query = """
            DECLARE @out int;
            EXEC [dbo].[p_SetCorrValuesMSCi] @AsOfDate = :p1, @PortfolioName = :p2, @Ticker = :p3, @SecName = :p4, @Quantity = :p5, @Price = :p6, @MktVal = :p7, @WeightMod = :p8, @MktCorr = :p9, @BmkCorr = :p10;
            SELECT @out AS the_output;
            """
            params = dict(p1= analysisDt.strftime(r'%m/%d/%y'), p2=portfolio, p3=sTicker, p4=sSecName, p5=fQuantity, p6=fPrice, p7=fMktVal, p8=fWeight, p9=fMktCorr, p10=fBmkCorr)
            with eng.connect() as conn:
                result = conn.execute(db.text(query), params).scalar()
                conn.commit()
                conn.close()

        # Runs a report based on the reportID obtained from the supported reports
        def RetrieveReportsSample(reportID, custTemplateName, custTemplateOwner, date, por, porOwner, analysisSetting, analysisSettingOwner, tsName, tsOwner):
            print('Retrieving report:', reportID, custTemplateName, custTemplateOwner)
            result = ''

            if reportID == 'CUSTOM':
                repTmp = client.factory.create('ReportTemplate')
                repTmp._TemplateName = custTemplateName
                repTmp._TemplateOwner = custTemplateOwner

                port = client.factory.create('InputPortfolio')
                port._Name = por
                port._Owner = porOwner

                porList = client.factory.create('Portfolios')
                porList.Portfolio = port

                aSetting = client.factory.create('InputAnalysisSettings')
                aSetting._Name = analysisSetting
                aSetting._Owner = analysisSettingOwner

                repDef = client.factory.create('RiskReportsDefinition')
                repDef.AnalysisDate = date
                repDef.Portfolios = porList
                repDef.AnalysisSettings = aSetting

                result = client.service.RetrieveReports(usr, cid, pwd, None, None, repDef, None, repTmp)

            if reportID == 'FACTOR_EXPOSURE' or reportID == 'RISK_ATTRIBUTION':
                repParams = client.factory.create('ReportParametersDef')
                repParams._ReportId = reportID

                port = client.factory.create('InputPortfolio')
                port._Name = por
                port._Owner = porOwner

                porList = client.factory.create('Portfolios')
                porList.Portfolio = port

                aSetting = client.factory.create('InputAnalysisSettings')
                aSetting._Name = analysisSetting
                aSetting._Owner = analysisSettingOwner

                repDef = client.factory.create('RiskReportsDefinition')
                repDef.AnalysisDate = date
                repDef.Portfolios = porList
                repDef.AnalysisSettings = aSetting

                result = client.service.RetrieveReports(usr, cid, pwd, None, None, repDef, repParams)

            if reportID == 'BPM_PA_TOTAL_RETURN_REPORT' or reportID == 'BPM_PA_TS_ATTRIBUTION_REPORT':
                port = client.factory.create('InputPortfolio')
                port._Name = por
                port._Owner = porOwner

                porList = client.factory.create('Portfolios')
                porList.Portfolio = port

                aSetting = client.factory.create('InputAnalysisSettings')
                aSetting._Name = analysisSetting
                aSetting._Owner = analysisSettingOwner

                ts = client.factory.create('InputTimeSeriesSettings')
                ts._Name = tsName
                ts._Owner = tsOwner

                repDef = client.factory.create('ReturnAttribution')
                repDef.TimeSeriesSettings = ts
                repDef.Portfolios = porList
                repDef.AnalysisSettings = aSetting

                paJob = client.service.SubmitReportJob(usr, cid, pwd, repDef)
                # print(paJob)
                taskId = paJob._TaskId
                jobId = paJob.JobId[0]

                print('Waiting for job...')
                print('TaskID: ', taskId)
                print('JobID:  ', jobId)

                time.sleep(3)

                jobStat = 30
                while jobStat == 30:
                    try:
                        jobStat = client.service.GetReportJobStatus(usr, cid, pwd, jobId)
                    except WebFault as detail:
                        print(detail)
                    except:
                        print("Unexpected error:", sys.exc_info()[0])
                        raise
                    if jobStat > 0:
                        time.sleep(jobStat / 2)

                if jobStat == -1:
                    print('Error!!')
                else:
                    repParams = client.factory.create('ReportParametersDef')
                    repParams._ReportId = reportID

                    result = client.service.RetrieveReports(usr, cid, pwd, taskId, None, None, repParams)

            if reportID == 'CUSTOM_TS':
                port = client.factory.create('InputPortfolio')
                port._Name = por
                port._Owner = porOwner

                porList = client.factory.create('Portfolios')
                porList.Portfolio = port

                aSetting = client.factory.create('InputAnalysisSettings')
                aSetting._Name = analysisSetting
                aSetting._Owner = analysisSettingOwner

                ts = client.factory.create('InputTimeSeriesSettings')
                ts._Name = tsName
                ts._Owner = tsOwner

                repDef = client.factory.create('ReturnAttribution')
                repDef.TimeSeriesSettings = ts
                repDef.Portfolios = porList
                repDef.AnalysisSettings = aSetting

                paJob = client.service.SubmitReportJob(usr, cid, pwd, repDef)
                taskId = paJob._TaskId
                jobId = paJob.JobId[0]

                print('Waiting for job...')
                print('TaskID: ', taskId)
                print('JobID:  ', jobId)

                time.sleep(3)

                jobStat = 30
                while jobStat == 30:
                    try:
                        jobStat = client.service.GetReportJobStatus(usr, cid, pwd, jobId)
                    except WebFault as detail:
                        print(detail)
                    except:
                        print("Unexpected error:", sys.exc_info()[0])
                        raise
                    if jobStat > 0:
                        time.sleep(jobStat / 2)

                if jobStat == -1:
                    print('Error! The job has failed. To view the log please rerun from the BPM UI.')
                    sys.exit()
                else:
                    repTmp = client.factory.create('ReportTemplate')
                    repTmp._TemplateName = custTemplateName
                    repTmp._TemplateOwner = custTemplateOwner

                    repParams = client.factory.create('RetrieveReportsInputParams')
                    repParams.TaskId = taskId
                    repParams.ReportTemplate = repTmp

                    result = client.service.RetrieveTemplateReports(usr, cid, pwd, repParams)
                    result = result.Response
                    print(result)

            for item in result:
                for data in item:
                    if data != "ExportJobReport":
                        df = data[0].ReportBody.ReportBodyGroup[0].ReportBodyRow
                        dx = data[1].ReportBody.ReportBodyGroup[0].ReportBodyRow

            for pos in dx:
                sTicker = pos.CellData[1]._Value
                sSecName = pos.CellData[2]._Value
                fQuantity = pos.CellData[3]._Value
                fPrice = pos.CellData[4]._Value
                fMktVal = pos.CellData[5]._Value
                fWeight = pos.CellData[6]._Value
                fMktCorr = pos.CellData[7]._Value
                fBmkCorr = pos.CellData[8]._Value        

                if sSecName != "":
                    if fPrice != "N/A":
                        LoadDataToDatabase(sTicker,  sSecName, fQuantity, fPrice, fMktVal, fWeight, fMktCorr, fBmkCorr)
                        print(str(sTicker) + " " + str(sSecName) + " " + str(fQuantity)+ " " + str(fPrice)+ " " + str(fMktVal)+ " " + str(fWeight) + " " + str(fMktCorr) + " " + str(fBmkCorr))

            print("Response end, code complete.")

        file = "https://www.barraone.com/axis2/services/BDTService?wsdl"
        client = Client(file, location=file, timeout=5000, retxml=False)

        # Open connection to MSCI
        usr = "ITAdmin"
        pwd = "7GRYPnZQaVWnnLRzU9yd"
        cid = "rkvi74supd"

        # settings
        conn_str = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:allostery-dev.database.windows.net,1433;Database=Operations;UID=svcOperations;PWD=Man@ger22!;MultipleActiveResultSets=False;Trusted_Connection=no;Auto_Commit=yes' 
        params = parse.quote_plus(conn_str)
        engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
        connection = engine.connect()

        dn = pd.read_sql("EXEC [dbo].[p_GetAMFNavValues]", engine)
        analysisDt = dn.AsOfDate.values[0]
        connection.close()

        analysisDt = dt.datetime(2024,4,22)
        portfolio = 'EFMGEMLT Estimation Universe'
        portfolioOwner = usr
        analysisSetting = "EFMGEMLTS-Gross"
        analysisSettingOwner = usr
        timeSeriesName = "tsSettings"
        timeSeriesOwner = usr
        customTemplate = "AMF-Estimation-Universe"
        customTemplateOwner = usr

        try:    
            # USE THIS ONE    
            RetrieveReportsSample("CUSTOM", customTemplate, customTemplateOwner, analysisDt, portfolio, portfolioOwner, analysisSetting, analysisSettingOwner, None, None)
            
        except WebFault as detail:
            logging.exception("getResutsFromMsci WebFault exception: %s", detail)
            print(detail)
        except Exception as e:
            logging.exception("getResutsFromMsci function failed with exception: %s", sys.exc_info()[0])
            print("getResutsFromMsci function failed with exception: %s", sys.exc_info()[0])

        except:
            logging.exception("getResutsFromMsci function failed with and unexpected error: %s", sys.exc_info()[0])
            print("getResutsFromMsci function failed with and unexpected error:", sys.exc_info()[0])
            raise

        print("All ok, script completed.")
    
