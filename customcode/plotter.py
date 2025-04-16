import altair as alt
import pandas as pd

def pie_chart_by_year(df, year):
    color_palette = [
        '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
        '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf',
        '#aec7e8', '#ffbb78', '#98df8a', '#ff9896', '#c5b0d5',
        '#c49c94', '#f7b6d2', '#c7c7c7', '#dbdb8d', '#9edae5'
    ]
    data = df[df['Year'] == year]

    return alt.Chart(data).mark_arc().encode(
        theta=alt.Theta(field="Percentage", type="quantitative"),
        color=alt.Color(field="Language", type="nominal", scale=alt.Scale(range=color_palette), sort=alt.SortField(field="Percentage", order="descending")),
        tooltip=["Language", "Percentage"]
    ).properties(
        title=f"Language distribution in {year}",
        width=400,
        height=400
    )