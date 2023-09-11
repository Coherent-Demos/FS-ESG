import streamlit as st
st.set_page_config(layout="wide")

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.figure_factory as ff
import requests
import json
import get_ticker_data as Yahoo


@st.cache_data
def callSparkModel(Xinput_controversial_flags, Xinput_environmental_weights, Xinput_governance_weights, Xinput_social_weights, Xinput_esg_weights, Xinput_TargetScore):

    # url = "https://excel.staging.coherent.global/coherent/api/v3/folders/ESG%20Investment%20Screening/services/ESG%20Screening%20and%20Analysis/execute"

    # payload = json.dumps({
    #    "request_data": {
    #       "inputs": {
    #          "ControversialActivites_Flags": Xinput_controversial_flags,
    #          "EnvironmentalScores_Weighting": Xinput_environmental_weights,
    #          "GovernanceScores_Weighting": Xinput_governance_weights, 
    #          "SocialScore_Weighting": Xinput_social_weights,
    #          "CustomESGWeights": Xinput_esg_weights
    #       }
    #    },
    #     "request_meta": {
    #         "compiler_type": "Neuron",
    #     }
    # })
    # headers = {
    #    'Content-Type': 'application/json',
    #    'x-tenant-name': 'coherent',
    #    'x-synthetic-key': 'facaae76-30e7-4201-9cc7-683dd3a751c6'
    # }

    url = "https://excel.uat.us.coherent.global/coherent/api/v3/folders/Spark FE Demos/services/ESG Screening and Analysis v2/Execute"

    payload = json.dumps({
       "request_data": {
          "inputs": {
             "ControversialActivites_Flags": Xinput_controversial_flags,
             "EnvironmentalScores_Weighting": Xinput_environmental_weights,
             "GovernanceScores_Weighting": Xinput_governance_weights, 
             "SocialScore_Weighting": Xinput_social_weights,
             "CustomESGWeights": Xinput_esg_weights,
             "TargetScore": Xinput_TargetScore
          }
       },
        "request_meta": {
            "compiler_type": "Neuron",
        }
    })
    headers = {
       'Content-Type': 'application/json',
       'x-tenant-name': 'coherent',
       'SecretKey': '2277565c-9fad-4bf4-ad2b-1efe5748dd11'
    }


    response = requests.request("POST", url, headers=headers, data=payload, allow_redirects=False)
    return response

@st.cache_data
def callESGFactorModel():

    url = "https://excel.uat.us.coherent.global/coherent/api/v3/folders/ESG%20Investment%20Screening/services/ESG%20Business%20Rules%20(SP500%20Sample)/execute"

    payload = json.dumps({
        "request_data": {
            "inputs": {}
        },
        "request_meta": {
            "compiler_type": "Neuron",
        }
    })
    headers = {
        'Content-Type': 'application/json',
        'x-tenant-name': 'coherent',
        'x-synthetic-key': 'eecee262-6c42-4219-beb7-19362ef9697b'
    }

    response = requests.request("POST", url, headers=headers, data=payload, allow_redirects=False)
    outputs = json.loads(response.text)['response_data']['outputs']
    return outputs

def process_series(series_a, series_b):
    series_back = []
    series_front = []

    for a_item, b_item in zip(series_a, series_b):
        x = a_item["value"]
        y = b_item["value"]

        if x > y:
            back_value = x
            front_value = y
            back_color = '#FF005C'
            front_color = '#EFEFEF'
        else:
            back_value = y
            front_value = x
            back_color = '#00D37A'
            front_color = '#EFEFEF'
        
        series_back.append({'name': a_item["name"], 'value': back_value, 'color': back_color})
        series_front.append({'name': a_item["name"], 'value': front_value, 'color': front_color})

    return series_back, series_front

def generatePortfolioChart(dataframe, portfoliofocus):

    columns_to_rename_a = {
        "Economic Sector": "name",
        "Portfolio Breakdown": "value"
    }
    # Extract specific columns
    selected_columns_df = dataframe[list(columns_to_rename_a.keys())]
    # Rename the extracted columns
    selected_columns_df = selected_columns_df.rename(columns=columns_to_rename_a)
    # Convert the selected columns DataFrame to a JSON array
    series_a = selected_columns_df.to_json(orient="records")


    columns_to_rename_b = {
        "Economic Sector": "name"
    }
    columns_to_rename_b[portfoliofocus] = "value"
    # Extract specific columns
    selected_columns_df = dataframe[list(columns_to_rename_b.keys())]
    # Rename the extracted columns
    selected_columns_df = selected_columns_df.rename(columns=columns_to_rename_b)
    # Convert the selected columns DataFrame to a JSON array
    series_b = selected_columns_df.to_json(orient="records")

    series_back, series_front = process_series(json.loads(series_a), json.loads(series_b))

    fig = go.Figure()

    for back_item, front_item in zip(series_back, series_front):
        fig.add_trace(go.Bar(
            y=[back_item['name']],
            x=[back_item['value']],
            orientation='h',
            marker_color=back_item['color'],
            offsetgroup=0,
            showlegend=False
        ))

        fig.add_trace(go.Bar(
            y=[front_item['name']],
            x=[front_item['value']],
            orientation='h',
            marker_color=front_item['color'],
            offsetgroup=1,
            showlegend=False
        ))

    fig.update_layout(
        barmode='overlay',
        yaxis=dict(categoryorder='total ascending'),
        xaxis_title='% of Portfolio',
        yaxis_title='',
        height=800,
        margin=dict(t=0)
    )

    return fig

def getNewESGScore(portfoliofocus):

    total_sum = sum(obj["Modified Weight"] * obj["Modified Score"] for obj in portfoliofocus)
    return total_sum


@st.cache_data
def defineInputTables():    
    economic_sectors = ['Retail Trade', 'Electronic Technology', 'Producer Manufacturing', 'Finance', 'Health Technology',
                        'Technology Services', 'Health Services', 'Consumer Non-Durables', 'Distribution Services',
                        'Consumer Durables', 'Process Industries', 'Consumer Services', 'Industrial Services', 
                        'Commercial Services', 'Transportation', 'Utilities', 'Non-Energy Minerals', 'Energy Minerals']
    
    contro_flag = ['Controversial_Weapons_Flag', 'Defence_Flag', 'Firearms_Flag', 'Fossil_Fuels_Flag', 'Nuclear_Flag']
    controversial_flags = pd.DataFrame(columns=contro_flag)
    controversial_flags['Economic Sector'] = economic_sectors
    controversial_flags.fillna(False, inplace=True)

    env_weightcols = ['EMISSIONS', 'ENVIRONMENTAL MANAGEMENT', 'WASTE', 'ENVIRONMENTAL STEWARDSHIP', 'RESOURCE_USE', 'WATER', 'ENVIRONMENTAL SOLUTIONS']
    envweights_df = pd.DataFrame(columns=env_weightcols)
    envweights_df['Economic Sector'] = economic_sectors
    envweights_df['EMISSIONS'] = 0.20
    envweights_df['ENVIRONMENTAL MANAGEMENT'] = 0.15
    envweights_df['WASTE'] = 0.05
    envweights_df['ENVIRONMENTAL STEWARDSHIP'] = 0.05
    envweights_df['RESOURCE_USE'] = 0.17
    envweights_df['WATER'] = 0.20
    envweights_df['ENVIRONMENTAL SOLUTIONS'] = 0.18

    soc_weightcols = ['Diversity', 'Occupational Health and Safety', 'Training and Development', 'Product Access Providing', 'Community Relations', 'Product Quality and Safety', 'Human Rights', 'Labour Rights', 'Compensation', 'Employment Quality']
    socweights_df = pd.DataFrame(columns=soc_weightcols)
    socweights_df['Economic Sector'] = economic_sectors
    socweights_df['Diversity'] = 0.15
    socweights_df['Occupational Health and Safety'] = 0.15
    socweights_df['Training and Development'] = 0.08
    socweights_df['Product Access Providing'] = 0.10
    socweights_df['Community Relations'] = 0.08
    socweights_df['Product Quality and Safety'] = 0.15
    socweights_df['Human Rights'] = 0.10
    socweights_df['Labour Rights'] = 0.10
    socweights_df['Compensation'] = 0.05
    socweights_df['Employment Quality'] = 0.05

    gov_weightcols = ['Business Ethics', 'Corporate Governance', 'Transparency', 'Forensic Accounting', 'Capital Structure']
    govweights_df = pd.DataFrame(columns=gov_weightcols)
    govweights_df['Economic Sector'] = economic_sectors
    govweights_df['Business Ethics'] = 0.20
    govweights_df['Corporate Governance'] = 0.15
    govweights_df['Transparency'] = 0.15
    govweights_df['Forensic Accounting'] = 0.25
    govweights_df['Capital Structure'] = 0.25

    esg_weightcols = ['E', 'S', 'G']
    esgweights_df = pd.DataFrame(columns=esg_weightcols)
    esgweights_df['Economic Sector'] = economic_sectors
    esgweights_df['E'] = 0.5
    esgweights_df['S'] = 0.3
    esgweights_df['G'] = 0.2


    return envweights_df, controversial_flags, socweights_df, govweights_df, esgweights_df

tab1, tab2, tab3, tab4 = st.tabs(['ESG Analyser', 'ESG Preferencial Screener', 'Custom Weight ESG', 'Portfolio Analytics'])

@st.cache_data
def runAggregation():
    esg_factors = callESGFactorModel()
    esg_df = pd.DataFrame(esg_factors['aggregatedESGFactors'])
    return esg_df

#initialize
customESGScore = 0
PortfolioAnalysis_Chart_df = pd.DataFrame([{}])
Xinput_TargetScore = 30

with tab1:

    esg_df = runAggregation()
    company  = st.selectbox('Choose a Company', esg_df['name'])
    
    st.markdown("***")

    selected_ticker = esg_df[esg_df['name'] == company]
    ticker_historical_data = Yahoo.get_historical_data(selected_ticker.iloc[0]['symbol'])
    
    esg_scores = selected_ticker[['esg', 'esg_e', 'esg_s', 'esg_g']].T
    esg_scores.columns = ['value']
    
    env_features = selected_ticker[['Emissions', 'Environmental Management', 'Waste', 'Environmental Stewardship', 'Resource Use', 'Water', 'Environmental Solutions']].T
    env_features.columns = ['value']
    env_features.sort_values(by="value", inplace=True)

    social_features = selected_ticker[['Diversity', 'Occupational Health and Safety', 'Training and Development', 'Product Access Providing', 'Community Relations', 'Product Quality and Safety', 'Human Rights', 'Labour Rights', 'Compensation', 'Employment Quality']].T
    social_features.columns = ['value']
    social_features.sort_values(by='value', inplace=True)

    gov_features = selected_ticker[['Business Ethics', 'Corporate Governance', 'Transparency', 'Forensic Accounting', 'Capital Structure']].T
    gov_features.columns = ['value']
    gov_features.sort_values(by='value', inplace=True)    

    global_compact = selected_ticker[['gc_score', 'gc_human_rights', 'gc_labour_rights', 'gc_environment', 'gc_anti_corruption']].T
    global_compact.columns = ['value']

    col1, col2, col3, col4 = st.columns(4)
    col1.metric(label="TICKER", value=selected_ticker.iloc[0]['symbol'])
    col2.metric(label="REGION", value=selected_ticker.iloc[0]['dom_region'])
    col3.metric(label="ECONOMIC SECTOR", value=selected_ticker.iloc[0]['economic_sector'])
    col4.metric(label="INDUSTRY", value=selected_ticker.iloc[0]['industry'])
    col1.metric(label="ESG", value=str(selected_ticker.iloc[0]['esg']))
    col2.metric(label="ESG Environment", value=str(selected_ticker.iloc[0]['esg_e']))
    col3.metric(label="ESG Social", value=str(selected_ticker.iloc[0]['esg_s']))
    col4.metric(label="ESG Governance", value=str(selected_ticker.iloc[0]['esg_g']))
    
    st.markdown("***")

    col1, col2 = st.columns(2)
    with col1:
        esg_fig = px.bar(x=["ESG", "ESG Environment", "ESG Social", "ESG Governance"], y=esg_scores['value'], title="ESG Score Breakdown", text=esg_scores['value'], labels={'y':'Score', 'x':'Breakdown'})
        esg_fig.update_traces(texttemplate='%{text:.4s}', textposition='outside')
        st.plotly_chart(esg_fig, use_container_width=True)

        esgE_fig = px.bar(x=env_features.index, y=env_features['value'], text=env_features['value'], title="ESG Environmental Features Breakdown", labels={'y':'Environmental Features', 'x':'Score'})
        esgE_fig.update_traces(texttemplate='%{text:.4s}', textposition='outside')
        st.plotly_chart(esgE_fig, use_container_width=True)

        esgG_fig = px.bar(x=gov_features.index, y=gov_features['value'], text=gov_features['value'], title="ESG Governance Features Breakdown", labels={'y':'Governance Features', 'x':'Score'})
        esgG_fig.update_traces(texttemplate='%{text:.4s}', textposition='outside')
        st.plotly_chart(esgG_fig, use_container_width=True)

    
    with col2:
        olhc_fig = px.line(ticker_historical_data, x=ticker_historical_data.index, y="Adj Close", title = "Historical Adjusted Close")
        olhc_fig.update_xaxes(minor=dict(ticks="inside", showgrid=True))
        st.plotly_chart(olhc_fig, use_container_width=True)

        esgS_fig = px.bar(x=social_features.index, y=social_features['value'], text=social_features['value'], title="ESG Social Features Breakdown", labels={'y':'Social Features', 'x':'Score'})
        esgS_fig.update_traces(texttemplate='%{text:.4s}', textposition='outside')
        st.plotly_chart(esgS_fig, use_container_width=True)

        gc_fig = px.bar(x=['GC Total', 'Human Rights', 'Labour Rights', 'Environment', 'Anti-Corruption'], y=global_compact['value'], text=global_compact['value'], 
        title="United Nation Global Compact Score Breakdown", labels={'x':'Score', 'y':'Global Compact Features'})
        gc_fig.update_traces(texttemplate='%{text:.4s}', textposition='outside')
        st.plotly_chart(gc_fig, use_container_width=True)
    
with tab2:

    markdown_about_filter = """

    ### ESG Investment Analysis - The Controversial Assets Filter can be instrumental for basic ESG Investment Analysis:
    - Negative Screening: The Controversial Assets Filter is a simple and easy-to-use tool that allows investors and asset managers to quickly screen out companies against their own corporate values or investment alignment. 
    - Institutional Investors: Investors can check their portfolio holdings’ alignment to their bespoke values and preferences criteria. Investors with high level of ESG integration can use the tool to quickly understand ESG impact on model portfolios.
    - Company Insights And Engagement: The Preferences Filter can highlight those companies involved in potentially controversial activities and thus which companies could be singled out for engagement campaigns.

    ### Spark API Call
    - The API call to the Spark service includes the preference filters as inputs to the screener model.
    - The API call to the Spark service returns the list of assets from your portfolio which match and don't match the preference filters.
    
    """

    st.markdown(markdown_about_filter)

    envweights_df, controversial_flags, socweights_df, govweights_df, esgweights_df = defineInputTables()
    contro_flag = ['Controversial_Weapons_Flag', 'Defence_Flag', 'Firearms_Flag', 'Fossil_Fuels_Flag', 'Nuclear_Flag']


    st.subheader('Screen for companies that match a selected combination of ESG filters')
    st.data_editor(controversial_flags)
        
    button = st.button('Run Preferencial Screening', type='primary')

    if button:
        
        response = callSparkModel(controversial_flags.to_dict('records'), envweights_df.to_dict('records'), govweights_df.to_dict('records'), socweights_df.to_dict('records'), esgweights_df.to_dict('records'), Xinput_TargetScore)
        outputs = json.loads(response.text)['response_data']['outputs']
        
        Non_Controversial_Assets_df = pd.DataFrame(outputs['Non_Controversial_Assets'])
        ControversialAssets_df = pd.DataFrame(outputs['ControversialAssets'])
        
        st.header("Non Controversial Assets")
        st.write(Non_Controversial_Assets_df)

        st.header("Controversial Assets")
        st.write(ControversialAssets_df)
    
with tab3:

    markdown_about_weighting = """

    ### ESG Investment Analysis - Build and test your custom weighted ESG scores on an economic sector level:
    - Define a custom weight for each of the factors broken by Environmental, Social and Governance.
    - Make sure the factor weights add up to 1.0.
    - Call the Spark service with the custom weights as the model inputs.

    ### Spark API Call
    - The API call to the Spark service includes the custom weights as inputs to the scoring model.
    - The API call to the Spark service returns the list of economic sectors from your portfolio with the custom ESG score and the ESG score from the data provider.
    
    """

    st.markdown(markdown_about_weighting)
    
    weighting_inputs = st.selectbox('Select Weighting Rules:', ['Environmental Factor Weights', 'Social Factor Weights', 'Governance Factor Weights', 'Overall ESG Weights'])
    if weighting_inputs == 'Environmental Factor Weights':
        st.data_editor(envweights_df)
    elif weighting_inputs == 'Social Factor Weights':
        st.data_editor(socweights_df)
    elif weighting_inputs == 'Governance Factor Weights':
        st.data_editor(govweights_df)
    elif weighting_inputs == 'Overall ESG Weights':
        st.data_editor(esgweights_df)

    button = st.button('Calculate Custom ESG Scores', type='primary')

    if button:
        response = callSparkModel(controversial_flags.to_dict('records'), envweights_df.to_dict('records'), govweights_df.to_dict('records'), socweights_df.to_dict('records'), esgweights_df.to_dict('records'), Xinput_TargetScore)
        outputs = json.loads(response.text)['response_data']['outputs']

        CustomScoringBreakdown_df = pd.DataFrame(outputs['CustomScoringBreakdown'])
        PortfolioESGScore = outputs['PortfolioESGScore']
        CustomESGScore = outputs['CustomESGScore']

        CustomScoringBreakdown_df.sort_values(by=['Number of Assets'], inplace=True, ascending=False)

        st.metric(label='Portfolio ESG Score', value=PortfolioESGScore)
        st.metric(label='Custom Weighted ESG Score', value=CustomESGScore)

        port_fig = px.bar(x=CustomScoringBreakdown_df['Economic Sector'], y=CustomScoringBreakdown_df['Number of Assets'], 
        title="Economic Sector Breakdown", text=CustomScoringBreakdown_df['Number of Assets'], 
        labels={'x':'Economic Sectors', 'y':'# of Assets'}, height=700)
        port_fig.update_traces(texttemplate='%{text:.0s}', textposition='outside')
        st.plotly_chart(port_fig, use_container_width=True)

        esg_fig = go.Figure(data=[
            go.Bar(name='ESG Score', x=CustomScoringBreakdown_df['Economic Sector'], y=CustomScoringBreakdown_df['ESG Score']), 
            go.Bar(name='Custom ESG Score', x=CustomScoringBreakdown_df['Economic Sector'], y=CustomScoringBreakdown_df['Custom ESG Score'])
        ])
        esg_fig.update_layout(barmode='group')
        st.plotly_chart(esg_fig, use_container_width=True)

        e_fig = go.Figure(data=[
            go.Bar(name='Environmental Score', x=CustomScoringBreakdown_df['Economic Sector'], y=CustomScoringBreakdown_df['Environmental Score']), 
            go.Bar(name='Custom Environmental Score', x=CustomScoringBreakdown_df['Economic Sector'], y=CustomScoringBreakdown_df['Custom Environmental Score'])
        ])
        e_fig.update_layout(barmode='group')
        st.plotly_chart(e_fig, use_container_width=True)
    
        s_fig = go.Figure(data=[
            go.Bar(name='Social Score', x=CustomScoringBreakdown_df['Economic Sector'], y=CustomScoringBreakdown_df['Social Score']), 
            go.Bar(name='Custom Social Score', x=CustomScoringBreakdown_df['Economic Sector'], y=CustomScoringBreakdown_df['Custom Social Score'])
        ])
        s_fig.update_layout(barmode='group')
        st.plotly_chart(s_fig, use_container_width=True)

        g_fig = go.Figure(data=[
            go.Bar(name='Governance Score', x=CustomScoringBreakdown_df['Economic Sector'], y=CustomScoringBreakdown_df['Governance Score']), 
            go.Bar(name='Custom Governance Score', x=CustomScoringBreakdown_df['Economic Sector'], y=CustomScoringBreakdown_df['Custom Governance Score'])
        ])
        g_fig.update_layout(barmode='group')
        st.plotly_chart(g_fig, use_container_width=True)

with tab4:
    st.write('### Optimize your Portfolio to Achieve Your target ESG Score')
    st.text("‎")

    col41, col42 = st.columns([3,9])
    with col41:
        with st.form("DC Form"):
            st.write("**Set your target score**")
            CurrentScoreValue = customESGScore if customESGScore else 60
            CurrentScore = st.metric("Current Portfolio Score", value=CurrentScoreValue)
            st.write('***')
            Xinput_TargetScore = st.number_input("Target Portfolio Score", min_value=30, value=CurrentScoreValue + 5)
            PortfolioFocus = st.selectbox('Portfolio Focus', ['Technology', 'Energy', 'Consumer'])
            st.markdown(
              """
              <style>
              .stButton>button {
                  background-color: blue;
                  color: white;
                  margin-top: 12px
              }
              .stButton>button:hover {
                  color: white;
              }
              .stButton>button:active {
                  color: white;
              }
              .stButton>button:focus {
                  color: white;
              }
              </style>
              """,
              unsafe_allow_html=True
            )

            DCbutton_clicked = st.form_submit_button("Optimize Portfolio", use_container_width=True)
            if DCbutton_clicked:
                response = callSparkModel(controversial_flags.to_dict('records'), envweights_df.to_dict('records'), govweights_df.to_dict('records'), socweights_df.to_dict('records'), esgweights_df.to_dict('records'), Xinput_TargetScore)
                outputs = json.loads(response.text)['response_data']['outputs']

                economic_sectors = [item["Economic Sector"] for item in outputs["CustomScoringBreakdown"]]
                portfolio_breakdowns = [item["Portfolio Breakdown"] for item in outputs["CustomScoringBreakdown"]]
                energy_new_breakdown = [item["Modified Weight"] for item in outputs["Energy"]]
                technology_new_breakdown = [item["Modified Weight"] for item in outputs["Technology"]]
                consumer_new_breakdown = [item["Modified Weight"] for item in outputs["Consumer"]]

                PortfolioAnalysis_Chart_df = pd.DataFrame({
                    "Economic Sector": economic_sectors,
                    "Portfolio Breakdown": portfolio_breakdowns,
                    "Energy": energy_new_breakdown,
                    "Technology": technology_new_breakdown,
                    "Consumer": consumer_new_breakdown,
                })
    with col42:
        with st.expander("**Portfolio Breakdown**", expanded=True):

            col14, col15, col16 = st.columns([1,1,1])
            with col14:
                NEW_ESG_Score = st.empty()
            st.write('***')

            PortfolioChart_placeholder = st.empty()

            response = callSparkModel(controversial_flags.to_dict('records'), envweights_df.to_dict('records'), govweights_df.to_dict('records'), socweights_df.to_dict('records'), esgweights_df.to_dict('records'), Xinput_TargetScore)
            outputs = json.loads(response.text)['response_data']['outputs']

            economic_sectors = [item["Economic Sector"] for item in outputs["CustomScoringBreakdown"]]
            portfolio_breakdowns = [item["Portfolio Breakdown"] for item in outputs["CustomScoringBreakdown"]]
            energy_new_breakdown = [item["Modified Weight"] for item in outputs["Energy"]]
            technology_new_breakdown = [item["Modified Weight"] for item in outputs["Technology"]]
            consumer_new_breakdown = [item["Modified Weight"] for item in outputs["Consumer"]]

            PortfolioAnalysis_Chart_df = pd.DataFrame({
                "Economic Sector": economic_sectors,
                "Portfolio Breakdown": portfolio_breakdowns,
                "Energy": energy_new_breakdown,
                "Technology": technology_new_breakdown,
                "Consumer": consumer_new_breakdown,
            })

            NEW_ESG_Score_value = getNewESGScore(outputs[PortfolioFocus])

            PortfolioChartFig = generatePortfolioChart(PortfolioAnalysis_Chart_df, PortfolioFocus)
            PortfolioChart_placeholder.plotly_chart(PortfolioChartFig, use_container_width=True)
            NEW_ESG_Score.metric("New Portfolio Score", round(NEW_ESG_Score_value, 2))

