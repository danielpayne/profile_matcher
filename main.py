import base64
import datetime
import io

from dash import Dash, dcc, html, Input, Output, State, dash_table

import dash_bootstrap_components as dbc

from dash.exceptions import PreventUpdate

import pandas as pd

import pandas_gbq as gbq

## Helper Functions
def parse_contents(contents, filename):
    content_type, content_string = contents.split(',')

    decoded = base64.b64decode(content_string)

    if 'csv' in filename:

        # Try different separators
        try:
            # Assume that the user uploaded a CSV file
            raw_df = pd.read_csv(
                io.StringIO(decoded.decode('utf-16')), sep=',')

        except:
            # Assume that the user uploaded a CSV file
            raw_df = pd.read_csv(
                io.StringIO(decoded.decode('utf-16')), sep='\t')
    else:
        # Assume that the user uploaded an excel file
        raw_df = pd.read_excel(io.BytesIO(decoded), engine='openpyxl')

    return raw_df

def append_cameo(df, post_col):

    # now make a clean postcode col
    df['postcode_merge'] = df[post_col].map(lambda x: x.replace(' ', '') if pd.notna(x) else x)

    # get unique postcodes
    nec_postcodes = tuple(df['postcode_merge'].unique())

    # Load table
    sql_script = '''
    SELECT
    cam.postcode_clean as postcode_merge,
    cam.uk_cam,
    cam.ukg_cam,
    cam.amplify,
    cam.uSwitch_segment,
    cam.JM_Segment,
    cam.BasketRoom_segment,
    ag.age_group

    FROM

    HT_Strata.client_segment cam left join HT_Strata.cameo_mapper ag
    on cam.uk_cam = ag.uk_cam_transaction

    WHERE cam.postcode_clean in {}
    '''.format(nec_postcodes)

    # Load
    sec_cameo_table = gbq.read_gbq(sql_script, project_id='ht-big-query')

    # Now merge
    output_table = df.merge(sec_cameo_table, on='postcode_merge', how='left')

    # drop col
    output_table = output_table.drop('postcode_merge', 1)

    return output_table

    HT_logo = 'https://media.glassdoor.com/sqll/2541714/human-theory-squarelogo-1554715634927.png'


    # Build app
    app = Dash(__name__, external_stylesheets=[dbc.themes.SUPERHERO], suppress_callback_exceptions=True)
    app.config['suppress_callback_exceptions'] = True

    # Notifications
    stopword_notification = dbc.Button("No Stopwords Uploaded", id='stopword-notification', color = "warning",
                                   style = {'margin-left' : '100px', 'width' : '170px'})

    file_notification = dbc.Button("No File Uploaded", id='file-notification', color = "warning",
                                   style = {'margin-left' : '100px', 'width' : '150px'})

    token_notification = dbc.Button("No File Submitted",
                                    id='token-notification', color = "danger",
                                   style = {'margin-left' : '100px', 'width' : '200px'})

    # Alerts
    file_alert = dbc.Alert("You must select a column before download can begin",
                            id="alert-file", is_open=False,  dismissable=True,color="danger",
                          style = {'margin-left' : '15px'})

    token_alert = dbc.Alert("There is a problem",
                            id="alert-token", is_open=False,  dismissable=True,color="danger",
                          style = {'margin-left' : '15px'})

    search_bar = dbc.Row(
        [
         dbc.Col(dcc.Loading(token_notification, type = 'graph', fullscreen=True)),

        ],
        no_gutters=True,
        className="ml-auto flex-nowrap mt-3 mt-md-0",
        align="center",
    )

    navbar = dbc.Navbar(
        [
            html.A(
                # Use row and col to control vertical alignment of logo / brand
                dbc.Row(
                    [
                        dbc.Col(html.Img(src=HT_logo, height="40px"), width='auto'),
                        dbc.Col(dbc.NavbarBrand("Profile Matcher", className="ml-2", style={'padding-left':'100px'}), width='auto'),


                        dbc.Col([file_alert, token_alert])
                    ],
                    align="center",
                    no_gutters=True,
                ),

            ),
            dbc.NavbarToggler(id="navbar-toggler"),
            dbc.Collapse(search_bar, id="navbar-collapse", navbar=True),
        ],
        color="dark",
        dark=True,
    )


    # Hidden storage

    #storage_data = dcc.Store(id='data-store', storage_type='memory')
    #storage_stop = dcc.Store(id='stop-store', storage_type='memory')
    #storage_token = dcc.Store(id='token-store', storage_type='memory')

    # Upload body elements
    control_upload = dbc.Jumbotron(
        [
            html.H3("Profile Matcher Tool"),
            html.P(
                "Instructions for Appending Cameo Data.",
                className="lead",
            ),
            html.Hr(className="my-2"),
            dcc.Markdown(
                '''
                1. First add a file by dragging it to the input area or selecting a file.\n

                2. When this is done a preview of the table will appear after several seconds\n

                3. Select the column which has the Postcode information.\n

                4. Select whether you want CSV or Excel output.\n

                5. Download Enriched Table
                '''
            ),

        ],

    )

    upload_area = html.Div([
        html.H3("File Upload Area", style = {'margin' : '10px'}),
        dcc.Upload(
            id='upload-data',
            children=html.Div([
                'Drag and Drop or ',
                html.A('Select Files', style = {'color': 'blue', 'text-decoration': 'underline'})
            ]),
            style={
                'width': '100%',
                'height': '60px',
                'lineHeight': '160px',
                'borderWidth': '1px',
                'borderStyle': 'dashed',
                'borderRadius': '5px',
                'textAlign': 'center',
                'margin': '10px'
            },
            # Allow multiple files to be uploaded
            multiple=False
        ),
        html.H5('No Files Staged', id = 'stage-file', style = {'margin' : '10px'}),


        html.H4("Initial Table Preview", style = {'margin' : '10px'}),
        dash_table.DataTable(id='datatable-upload-container', style_data={
            'color': 'black',
            'backgroundColor': 'white'
        },style_header={
            'backgroundColor': 'rgb(210, 210, 210)',
            'color': 'black',
            'fontWeight': 'bold'
        },
        style_cell={'textAlign': 'left'},
        column_selectable="single",page_size=10, selected_columns=[]),
        html.Br(),
        html.H4("No Column selected", style = {'margin' : '10px'}, id='postcode-select-col'),
        html.Br(),
        dbc.RadioItems(
                options=[
                    {"label": "CSV Output", "value": 1},
                    {"label": "Excel Output", "value": 2}
                ],
                value=1,
                id="output-select",

                inline=True,
            ),
        dbc.Button("Download Enriched Table", id = 'download-csv-btn', n_clicks = 0, disabled=True,
                   style = {'display': 'inline-block', 'margin-right' : '20px', 'margin' : '10px'}),

        dcc.Download(id="download-dataframe-csv")



    ])

    body_upload = dbc.Container([
        dbc.Row(
                [
                    dbc.Col(control_upload, width=4, align="top"),
                    dbc.Col(upload_area, width=8, style = {'height' : '100%'}),
                ],
                style={"marginTop": 30},
            ),
    ], style={"height": "100vh"}, fluid=True)

    app.layout = html.Div([navbar,  storage_data,body_upload])






    @app.callback(Output('datatable-upload-container', 'data'),
                  Output('datatable-upload-container', 'columns'),
                  Input('upload-data', 'contents'),
                  State('upload-data', 'filename'))
    def update_output(contents, filename):
        if contents is None:
            return [{}], []
        df = parse_contents(contents, filename)
        return df.to_dict('records'), [{"name": i, "id": i,  "selectable": True} for i in df.columns]

    @app.callback(
        Output('postcode-select-col', 'children'),
        Input('datatable-upload-container', 'selected_columns')
    )
    def column_select(selected_columns):

        if selected_columns == []:
            raise PreventUpdate


        sel_cool = [i for i in selected_columns][0]
        return "Selected Column is: {}".format(sel_cool)

    @app.callback(
    Output("download-csv-btn", "disabled"),
    Input('datatable-upload-container', 'selected_columns'))
    def send_warning(sel_columns):

        if sel_columns == []:
            return True
        return False

    @app.callback(
    Output("download-dataframe-csv", "data"),
    Output("token-notification", "children"),
    Input("download-csv-btn", "n_clicks"),
    State('datatable-upload-container', 'data'),
    State('datatable-upload-container', 'selected_columns'),
    State('upload-data', 'filename'),
    State('output-select', 'value'))
    def execute_download(click,df_data, sel_columns, filename, output_type):

        if sel_columns == []:
            raise PreventUpdate

        # Identify postcode col
        post_col = [i for i in sel_columns][0]
        copy_table = pd.DataFrame(df_data)

        # Add data
        output_table = append_cameo(copy_table, post_col)

        if output_type == 1:

            new_filename = filename.split('.')[0] + '_cameo_enriched.csv'

            return dcc.send_data_frame(output_table.to_csv, new_filename, index=False), "Complete!!!"

        else:

            new_filename = filename.split('.')[0] + '_cameo_enriched.xlsx'

            return dcc.send_data_frame(output_table.to_excel, new_filename, sheet_name="Cameo Data", index=False), "Complete!!!"


    if __name__ == '__main__':
        app.run_server(debug=True, threaded=True)
