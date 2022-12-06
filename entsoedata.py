import requests 
import sqlite3
import pandas as pd 
from datetime import datetime as dt
from datetime import timedelta as td
from IPython.display import display 
from bs4 import BeautifulSoup  
import numpy as np 
import lxml
import matplotlib.pyplot as plt 
import scipy.stats as st 
from dataclasses import dataclass 
import julian 

@dataclass
class DayAheadPrices: 
    '''DayAheadPrices class makes the process of accessing day-ahead prices via 
    ENTSO-E RESTful API easier and gives you several options of visualization. 

    Attributes:
        zones: 'list', | Default: None.
            Represents one or several energyzones from the areas(countrys) 
            in list format.
        startdate: 'str', | Default: "20200101"
            First date of interval for request as string.
        enddate: 'str', optional | Default: Today's date. 
            Last date of interval for request as string.
        plottype: 'str', optional | Default: "Line" | Options: "Line", "Scatter", "Histogram"
            Type of visualization requested as string. 
        percentile: 'int', optional | Default: 100
            Percentile of the full dataset for the interval, combines with range.
        range: 'str', optional | Default: "Middle" | Options: "Top", "Middle", "Bottom"
            Range used with percentile to choose which data to keep as string. 
            Ex: 95 percentile and "Top" range results in a dataset representing 
                the 5 % highest values from the full dataset.
        groupby: 'str', optional | Default: Empty string | Options: "Day", "Month"
            Type of grouping of the dataset as string.

    Notes: Insert API-token in the method "load_db" before requesting data
           from DayAheadPrices.

    Examples of how to run in another script:
        from entsoedata import DayAheadPrices
        print(DayAheadPrices.__doc__)
        DayAheadPrices.info()
        DayAheadPrices(["SE_1","FI"],startdate="20200101",plottype="Scatter",
                        percentile=95,groupby="Day").get_analytics()


    Source: https://transparency.entsoe.eu/
    '''

    zones:list = None
    startdate: str = "20200101"
    enddate: str = dt.now().strftime('%Y%m%d')
    plottype: str = "Line"
    percentile: int = 100
    range: str = "Middle"
    groupby: str = ""


    def info(self):
        '''Displays shortcodes used as input to zones for the various 
        areas to choose from, along with other describing info.
        '''

        conn = sqlite3.connect("./Energyprices.db")
        try: 
            domaininfo = pd.read_csv("./Domaininfo.csv")
        except FileNotFoundError as fnf:
            print(fnf)
            print("""Could not find "Domaininfo.csv" to read in Domaininfo!""")
            raise
        try:
            domaininfo.to_sql("DomainInfo", conn, if_exists='fail', index=False)
        except ValueError:
            pass
        try:
            textinfo = pd.read_sql('''SELECT * FROM DomainInfo; ''', conn)
            conn.close()
            with pd.option_context('display.max_rows', None):
                display(textinfo.set_index('Shortcode'))
        except Exception:
            print("Can't display Domaininfo!")


    def load_db(self, zones=None):
        '''Loads the sqlite database with day-ahead prices 
        along with date and time for the choosen zones and inverval.

        Parameters: 
            zones: 'list', optional
        '''
        
        if zones is None:
            zones = self.zones
        #Insert your API-token below to be able to run script.
        api_token = "0c4d1c98-01a6-48bb-a44f-40a33d777dba"
        api_base = f"api?securityToken={api_token}"
        doc_type = "A44"
        start_day = dt.strptime(self.startdate, '%Y%m%d').date()
        end_day = dt.strptime(self.enddate, '%Y%m%d').date()
        interval = (end_day-start_day).days
        startdate = self.startdate
        enddate = self.enddate
        
        if interval < 0:
            print("The interval is invalid, the endingdate most be" 
                    "after the startingdate in time!") 
        
        elif interval > 369: 
            lower_limit = start_day
            upper_limit = start_day + td(days=369)
            while (end_day-lower_limit).days > 369:
                for zon in zones:
                    conn = sqlite3.connect("./Energyprices.db")
                    try:
                        cursor = conn.cursor()
                        cursor.execute(f'''
                            SELECT Domainstr 
                            FROM DomainInfo 
                            WHERE Shortcode="{zon}";
                            ''')
                        Domain = cursor.fetchone()[0]
                    except Exception:
                        print(f"{zon} is not a valid shortcode!")
                        continue
                    cursor.execute(f'''
                        CREATE TABLE IF NOT EXISTS "{zon}" 
                        (DateTime TEXT PRIMARY KEY, Price INT); 
                        ''')
                    conn.commit()
                    cursor.close()
                    
                    lower_limit_str = dt.strftime(lower_limit, '%Y%m%d')
                    upper_limit_str = dt.strftime(upper_limit, '%Y%m%d')
                    try:
                        response = requests.get(
                                        "https://web-api.tp.entsoe.eu/"\
                                        f"{api_base}&"\
                                        f"documentType={doc_type}&"\
                                        f"in_Domain={Domain}&"\
                                        f"out_Domain={Domain}&"\
                                        f"periodStart={lower_limit_str}0000&"\
                                        f"periodEnd={upper_limit_str}0000")
                    except requests.exceptions.RequestException as re:
                        print(re)
                        print(f"Request for {zon} {startdate}-{enddate} failed!")
                        continue
                    
                    response_content = BeautifulSoup(response.content, 'lxml')
                    try:
                        no_data=response_content.find('text').get_text()[0:22]
                        no_data_str='No matching data found'
                        if no_data == no_data_str:
                            print(f"No data found for {zon} {lower_limit_str} - "\
                                    f"{upper_limit_str}, choose another zone!")
                            exit(1) 
                    except Exception:
                        pass
                    response_selection = response_content.find_all('timeseries')
                    insert_query = f'''INSERT INTO "{zon}" (DateTime, Price) VALUES (?,?)'''
                    for day_timeseries in response_selection:
                        date = dt.strptime(
                        day_timeseries.find_all('start')[0].get_text(),'%Y-%m-%dT%H:%MZ'
                        )
                        try:
                            for hour in day_timeseries.find_all('point'):
                                conn = sqlite3.connect("./Energyprices.db")
                                hour_index = int(hour.find('position').get_text())
                                datehour = str(date + td(hours=hour_index))
                                price = hour.find('price.amount').get_text()
                                cursor = conn.cursor()
                                cursor.execute(insert_query, (datehour,price))
                                conn.commit()
                        except Exception:
                           pass
                    conn.close() 
                    print(f"{zon} fetched and loaded for {lower_limit}"\
                            f" - {upper_limit} in to sql tables!")
                lower_limit = upper_limit 
                upper_limit += td(days=369)
            startdate = dt.strftime(lower_limit, '%Y%m%d')
            enddate = dt.strftime(end_day, '%Y%m%d')
        
        for zon in zones:
                conn = sqlite3.connect("./Energyprices.db")
                try:
                    cursor = conn.cursor()
                    cursor.execute(f'''
                        SELECT Domainstr 
                        FROM DomainInfo 
                        WHERE Shortcode="{zon}"
                    ''')
                    Domain = cursor.fetchone()[0]
                except Exception:
                    print(f"{zon} is not a valid shortcode!")
                    continue
                cursor.execute(f'''
                    CREATE TABLE IF NOT EXISTS "{zon}" 
                    (DateTime TEXT PRIMARY KEY, Price INT) 
                    ''')
                conn.commit()
                cursor.close()
                try:
                    response = requests.get(
                                        "https://web-api.tp.entsoe.eu/"\
                                        f"{api_base}&"\
                                        f"documentType={doc_type}&"\
                                        f"in_Domain={Domain}&"\
                                        f"out_Domain={Domain}&"\
                                        f"periodStart={startdate}0000&"\
                                        f"periodEnd={enddate}0000")
                except requests.exceptions.RequestException as re:
                    print(re)
                    print(f"Request for {zon} {startdate}-{enddate} failed!")
                    continue
                response_content = BeautifulSoup(response.content, 'lxml')
                try:
                    no_data=response_content.find('text').get_text()[0:22]
                    no_data_str='No matching data found'
                    if no_data == no_data_str:
                        print(f"No data found for {zon} {lower_limit_str} - "\
                                    f"{upper_limit_str}, choose another zone!")
                        exit(1)
                except Exception:
                    pass
                response_selection = response_content.find_all('timeseries')
                insert_query = f'''INSERT INTO "{zon}" (DateTime, Price) VALUES (?,?)'''
                for day_timeseries in response_selection:
                    date = dt.strptime(
                        day_timeseries.find_all('start')[0].get_text(),
                        '%Y-%m-%dT%H:%MZ')
                    try:
                        for hour in day_timeseries.find_all('point'):
                            hour_index = int(hour.find('position').get_text())
                            datehour = str(date + td(hours=hour_index))
                            price = hour.find('price.amount').get_text()
                            cursor = conn.cursor()
                            cursor.execute(insert_query, (datehour,price))
                            conn.commit()
                            cursor.close()       
                    except Exception:
                        pass
                conn.close()
                print(f"{zon} fetched and loaded for {startdate}"\
                        f" - {enddate} in to sql tables!")


    def check_status_of_zones(self):
        '''Checks if the data for the zones and interval already exists in 
        the database. 
        Calls on the method for loading in data if a certain zone don't exists
        in the database.
        '''

        lower_limit_str = dt.strptime(self.startdate, 
                                        '%Y%m%d').strftime('%Y-%m-%d %H:%M')
        upper_limit_str = dt.strptime(self.enddate, 
                                        '%Y%m%d').strftime('%Y-%m-%d %H:%M')
        check=[]

        for zon in self.zones:
            try:
                conn = sqlite3.connect("./Energyprices.db")
                tableexists = pd.read_sql(f'''
                    SELECT strftime('%Y-%m-%d %H:%M',"{zon}".DateTime) AS DateHour, 
                    Price AS "{zon} Price" 
                    FROM "{zon}" 
                    WHERE DateHour BETWEEN "{lower_limit_str}" AND "{upper_limit_str}"; 
                    ''',conn)
                conn.close()
            except Exception:
                check.append(zon)
                continue
            start_day = dt.strptime(self.startdate, '%Y%m%d').date()
            end_day = dt.strptime(self.enddate, '%Y%m%d').date()
            interval_points = (end_day-start_day).days * 24 * 0.95
            if (tableexists.empty or 
                    (len(tableexists.index) < interval_points)):
                check.append(zon)
        if check:
            print(f"Fetching data for {check}")
            DayAheadPrices.load_db(self, zones=check)    
        

    def get_historydata(self):
        '''Queries the database and fetching data for the zones and groups the 
        if parameter groupby is change from default.
        Returns the data as a dataframe.

        Options for groupby: "Month", "Day".
        '''
        
        DayAheadPrices.check_status_of_zones(self)

        startdate = dt.strptime(self.startdate, '%Y%m%d').strftime('%Y-%m-%d %H:%M')
        enddate = dt.strptime(self.enddate, '%Y%m%d').strftime('%Y-%m-%d %H:%M')
        first_zon = self.zones[0]
        price_column_query = ""
        inner_join_query = ""
        groupby_query = ""

        if self.groupby == "Month":
            groupby_query = '''GROUP BY YearMonth'''
        elif self.groupby == "Day":
            groupby_query = '''GROUP BY YearMonthDay'''
        
        if len(self.zones) > 1:
            for zon in self.zones[1:]:
                price_column_query = (f'''{price_column_query},'''\
                    f''' "{zon}".Price AS "{zon} Price" ''')
                inner_join_query = (f'''{inner_join_query}INNER JOIN "{zon}" '''\
                    f'''ON "{zon}".DateTime = "{first_zon}".DateTime ''')
        
        insert_query=f''' 
            SELECT strftime('%Y-%m-%d %H',"{first_zon}".DateTime), 
            strftime('%Y%m',"{first_zon}".DateTime) AS YearMonth, 
            strftime('%Y%m%d',"{first_zon}".DateTime) AS YearMonthDay, 
            julianday("{first_zon}".DateTime) TimeStamp, 
            "{first_zon}".Price AS "{first_zon} Price"{price_column_query} 
            FROM "{first_zon}" 
            {inner_join_query} 
            WHERE "{first_zon}".DateTime BETWEEN "{startdate}" AND "{enddate}"  
            {groupby_query}
            ORDER BY "{first_zon}".DateTime ASC
            '''
        conn = sqlite3.connect("./Energyprices.db")
        try:
            historydata = pd.read_sql(insert_query,conn)
        except Exception as exc:
            conn.close()
            print(exc)
            print("Fetching of data for historydata failed!")
            raise
        else:
            if self.percentile != 100:
                percentile = self.percentile / 100
                QB = historydata.quantile(1 - percentile)
                QT = historydata.quantile(percentile)
                if self.range == "Middle":
                    historydata = historydata[~((historydata < QB) | (historydata > QT)).any(axis=1)]
                elif self.range == "Top":
                    historydata = historydata[~(historydata < QT).any(axis=1)]
                elif self.range == "Bottom":
                    historydata = historydata[~(historydata > QT).any(axis=1)]
                else:
                    print("Input to range is not a valid range!")
            return historydata


    def get_historytables(self):
        '''Displays the dataframe from historydata in terminal.'''
        historydata = DayAheadPrices.get_historydata(self)
        with pd.option_context('display.max_rows',None):
            display(historydata.set_index('YearMonth'))   


    def get_historyplots(self):
        '''Makes visualisation plots with the given dataframe from 
        get_historydata.
        
        Options for plottype: "Line", "Scatter", "Histogram".
        '''
 
        tableaucolors = ['#2ca02c', '#1f77b4', '#ff7f0e', '#aec7e8', '#ffbb78',
                        '#ff9896', '#c5b0d5', '#98df8a', '#9467bd', '#e377c2', 
                        '#bcbd22', '#9edae5', '#f7b6d2', '#17becf']
  
        zonesdata = DayAheadPrices.get_historydata(self) 

        number_of_subplots = len(self.zones) + 1
        plt.figure(f"Prices for {str(self.zones)[1:-1]} energy zones", 
            figsize = (10, 5), dpi = 150)
    
        if self.plottype == "Histogram":
            plt.subplot(number_of_subplots, 1, 1)
            for rank,zon in enumerate(self.zones):
                percentile_25,percentile_75 = np.percentile(zonesdata[f"{zon} Price"], [25, 75])
                bin_width = (2 
                            * (percentile_75-percentile_25) 
                            * len(zonesdata[f"{zon} Price"]) 
                            ** (-1/3))
                bins = round((zonesdata[f"{zon} Price"].max() 
                            - zonesdata[f"{zon} Price"].min())/bin_width)
                plt.hist(zonesdata[f"{zon} Price"], density=True, 
                        bins=bins, label=f"{zon}", alpha=0.3, 
                        color=tableaucolors[-(rank+1)], lw=0)
                xlim = plt.xlim()
                plt.xlim(xlim[0], xlim[1])
                kde_xs = np.linspace(xlim[0], xlim[1], 500)
                kde = st.gaussian_kde(zonesdata[f"{zon} Price"])
                plt.plot(kde_xs, kde.pdf(kde_xs), label=f"{zon} PDF", 
                        linewidth=1, alpha=0.7, color=tableaucolors[rank])
                plt.tight_layout()  
            plt.yticks(fontsize=5)
            plt.xticks(fontsize=5)
            plt.ylabel('Probability', fontsize=7)
            plt.legend(fontsize=7, frameon=False, ncol=(len(self.zones)*2))

            for rank,zon in enumerate(self.zones):
                plt.subplot(number_of_subplots, 1, (rank+2))
                percentile_25, percentile_75 = np.percentile(zonesdata[f"{zon} Price"], [25, 75])
                bin_width = (2 
                            * (percentile_75-percentile_25) 
                            * len(zonesdata[f"{zon} Price"]) 
                            ** (-1/3))
                bins = round((zonesdata[f"{zon} Price"].max() 
                            - zonesdata[f"{zon} Price"].min())/bin_width)
                plt.hist(zonesdata[f"{zon} Price"], density=True, bins=bins, 
                        label=f"{zon}", alpha=0.5, color=tableaucolors[-(rank+1)], 
                        lw=0.2, ec='White')  
                xlim = plt.xlim()
                plt.xlim(xlim[0], xlim[1])
                kde_xs = np.linspace(xlim[0], xlim[1], 500)
                kde = st.gaussian_kde(zonesdata[f"{zon} Price"])
                plt.plot(kde_xs, kde.pdf(kde_xs), label=f"{zon} PDF", linewidth=1, 
                        alpha=1, color=tableaucolors[rank])
                plt.tight_layout()
                plt.legend(fontsize=7, frameon=False, ncol=2)
                plt.yticks(fontsize=5)
                plt.xticks(fontsize=5)
                plt.ylabel('Probability', fontsize=7)
            plt.tight_layout()
            plt.xlabel('Price', fontsize=7)
        
        else:
            plt.subplot(number_of_subplots, 1, 1)
            for rank,zon in enumerate(self.zones):
                if self.plottype == "Line":
                    plt.plot(zonesdata.iloc[:,0], zonesdata[f"{zon} Price"], 
                            linewidth=0.7, label=f"{zon}", alpha=0.8, 
                            color=tableaucolors[rank])
                elif self.plottype == "Scatter":
                    plt.scatter(zonesdata.iloc[:,0], zonesdata[f"{zon} Price"], 
                                label=f"{zon}", alpha=0.6, c=tableaucolors[rank],
                                edgecolors='Black', linewidth=0.1, s=10)
                plt.yticks(fontsize=5)
                divider_of_xaxis = round(len(zonesdata.index) 
                                / 11 if len(zonesdata.index) > 11 else 5)
                plt.xticks(zonesdata.iloc[:,0][::divider_of_xaxis], fontsize=5)
                plt.ylabel('Price [EUR/MWh]',fontsize=7)
                plt.xlim(0,zonesdata.iloc[-1,0])
                plt.tight_layout()
            plt.legend(fontsize=7, frameon=False, ncol=len(self.zones))
            
            for rank,zon in enumerate(self.zones):
                plt.subplot(number_of_subplots, 1, (rank+2))
                if self.plottype == "Line":
                    #Plots the line plot.
                    plt.plot(zonesdata.iloc[:,0], zonesdata[f"{zon} Price"], 
                        linewidth=0.7, label=f"{zon}", color=tableaucolors[rank])
                elif self.plottype == "Scatter":
                    #Plots the scatter plot.
                    plt.scatter(zonesdata.iloc[:,0], zonesdata[f"{zon} Price"], 
                                label=f"{zon}", alpha=0.8, c=tableaucolors[rank],
                                edgecolors='Black', linewidth=0.1, s=10)
                plt.yticks(fontsize = 5)
                divider_of_xaxis = round(len(zonesdata.index) 
                                / 11 if len(zonesdata.index) > 11 else 5)
                plt.xticks(zonesdata.iloc[:,0][::divider_of_xaxis], fontsize=5)
                plt.xlim(0,zonesdata.iloc[-1,0])
                plt.legend(fontsize=7, frameon=False)
                plt.tight_layout()
                plt.ylabel('Price [EUR/MWh]',fontsize=7)
            plt.xlabel('YearMonth', fontsize=7)
        plt.tight_layout()
        plt.show()
        #For saving the plot as .png uncomment the line below. 
        #plt.savefig(f'./Lineplot for {str(zones)[1:-1]}.png ')


    def get_analytics(self):
        '''Makes visualisation of the dataframe from get_historytables 
        with linear regression, prediction range and confidence range.
        Also returns equation for the linear regression along with values 
        for R², T-score, P-value and alpha.   
        '''

        tableaucolors = ['#2ca02c', '#1f77b4', '#ff7f0e', '#aec7e8', '#ffbb78',
                        '#ff9896', '#c5b0d5', '#98df8a', '#9467bd', '#e377c2', 
                        '#bcbd22', '#9edae5', '#f7b6d2', '#17becf']

        zonesdata = DayAheadPrices.get_historydata(self)

        plt.figure(f"Analysis for energy zones: {str(self.zones)[1:-1]}", 
                    figsize=(10, 5), dpi=150)
        for rank,zon in enumerate(self.zones):
            time = zonesdata['TimeStamp']
            price = zonesdata[f"{zon} Price"]
            reg_koef = np.polyfit(time, price, 1) #Linear regression 
            number_prices = price.size
            number_koef = reg_koef.size
            deg_of_freedom = number_prices - number_koef
            alpha = 0.05
            tails = 2
            t_stat = st.t.ppf(1 - (alpha / tails), deg_of_freedom) # Student’s t
            p_value = st.t.sf(abs(t_stat), deg_of_freedom)
            y_model = np.polyval(reg_koef, time)
            model = np.poly1d(reg_koef)
            y_model = model(time)
            y_bar = np.mean(price)
            r_square = np.sum((y_model-y_bar) ** 2) / np.sum((price-y_bar) ** 2)
            resid = price - y_model
            std_err = np.sqrt(sum(resid ** 2) / deg_of_freedom)

            plt.subplot(len(self.zones), 1, rank+1)
            plt.scatter(time, price, c=tableaucolors[rank], marker='o', s = 3)
            xlim = plt.xlim()
            ylim = plt.ylim()
            plt.plot(np.array(xlim), (reg_koef[1]+reg_koef[0]*np.array(xlim)), 
                    c=tableaucolors[-1], linewidth=1, 
                    label = f'Linear regression line: y={reg_koef[0]:.4f}x'\
                        f' {"+" if reg_koef[1]>0 else "-"}{abs(reg_koef[1]):.2f}')
            time_fitted = np.linspace(xlim[0], xlim[1], 100)
            price_fitted = np.polyval(reg_koef, time_fitted)
            ci = (t_stat * std_err 
                * np.sqrt(1 / number_prices + (time_fitted-np.mean(time)) 
                        ** 2 / np.sum((time-np.mean(time)) ** 2)))
            plt.plot([],[],' ',
                label=f'R²: {r_square:.3f} | T-score: {t_stat:.3f} | P-value:'\
                    f' {p_value:.3f} | Alpha: {alpha}')
            plt.fill_between(time_fitted, (price_fitted+ci), (price_fitted-ci), 
                            label = f'95 % Confidence Interval',
                            facecolor='#b9cfe7', zorder=0)
            pi = (t_stat * std_err 
                * np.sqrt(1 + 1 / number_prices + (time_fitted - np.mean(time))
                        **2 / np.sum((time-np.mean(time)) ** 2)))
            plt.plot(time_fitted, (price_fitted-pi), '--', color='0.5', 
                label=f'95 % Prediction Limits')
            plt.plot(time_fitted, (price_fitted+pi), '--', color='0.5')
            def JDayToStr(element):
                return julian.from_jd(element).strftime('%Y-%m')
            mapping = list(map(JDayToStr,time))
            divider_of_xaxis = round(len(zonesdata.index) 
                                / 11 if len(zonesdata.index) > 11 else 5)
            plt.xticks(time[::divider_of_xaxis], mapping[::divider_of_xaxis],
                        fontsize = 5)
            plt.yticks(fontsize=5)
            plt.xlim(xlim)
            plt.ylim(0, ylim[1])
            plt.title(f"{zon}", fontsize=10, loc='left')
            plt.ylabel('Price [EUR/MWh]', fontsize=7)
            plt.legend(loc=(0.2,1.01), ncol=2, fontsize=4, frameon=False)
            plt.tight_layout()
        plt.xlabel('YearMonth', fontsize=7)
        plt.tight_layout()
        plt.show()
        #For saving the plot as .png uncomment the line below. 
        #plt.savefig(f'./Linearregression"{str(zones)[1:-1]}".png ')
