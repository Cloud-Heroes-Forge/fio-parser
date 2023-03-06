import json
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import jinja2

def generate_blocksize_summary(fio_results_df: pd.DataFrame) -> str:
    """Generates a summary of all the blocksize parameters in the given fio results dataframe.

    The summary includes the number of tests, the distinct blocksize values used in the tests, 
    and the range and median of the blocksize values.

    Args:
        fio_results_df (pd.DataFrame): A pandas DataFrame containing fio results.

    Returns:
        str: A string containing the blocksize summary.
    """
    blocksize_summary = ''
    blocksize_data = fio_results_df['blocksize'].unique()
    for blocksize in blocksize_data:
        data = fio_results_df[fio_results_df['blocksize'] == blocksize]
        blocksize_summary += f"For blocksize={blocksize}: \n"
        blocksize_summary += f"\t-Total Throughput: {data['total_bw'].mean():.2f} {data['bw_unit'].iloc[0]}\n"
        blocksize_summary += f"\t-Total IOPS: {data['total_iops'].mean():.2f}\n"
        blocksize_summary += f"\t-Average Latency: {data['latency_ms'].mean():.2f} ms\n"
    return blocksize_summary

def generate_total_throughput_chart(data: pd.DataFrame) -> plt.Figure:
    fig, ax1 = plt.subplots()

    # plot total throughput on the left y-axis
    ax1.bar(np.arange(len(data['total_throughput'])), data['total_throughput'], color='tab:blue')
    ax1.set_ylabel('Total Throughput', color='tab:blue')
    ax1.tick_params(axis='y', labelcolor='tab:blue')
    ax1.set_xticks(np.arange(len(data['total_throughput'])))
    ax1.set_xticklabels(data['xlabels'], rotation=90)

    # plot average latency on the right y-axis
    ax2 = ax1.twinx()
    ax2.plot(np.arange(len(data['avg_latency'])), data['avg_latency'], color='tab:orange')
    ax2.set_ylabel('Average Latency (ms)', color='tab:orange')
    ax2.tick_params(axis='y', labelcolor='tab:orange')

    plt.title('Total Throughput vs Blocksize')
    plt.tight_layout()
    return fig

def generate_fio_report(fio_results_df: pd.DataFrame, report_file_path: str) -> None:
    """Using the template.html file, generates an html report for the fio results.
        * overall summary of all of the `blocksize` parameters together
        * one page with 2 charts showing:
            ** y axis: Total Throughput, x axis: Blocksize, secondary y axis: avg_latency
            ** y axis: Total IOPS, x axis: Blocksize, secondary y axis: avg_latency
        * for each `blocksize`: 
            ** a paragraph summary 
            ** charts showing:
                *** y axis: Total Throughput, x axis: io_depth, secondary y axis: avg_latency
                *** y axis: Total IOPS, x axis: io_depth, secondary y axis: avg_latency
    Args:
        fio_results (pd.DataFrame): list of fio results
        report_file_path (str): path to report file

    Returns:
        None
    """
    # load the template
    template_loader = jinja2.FileSystemLoader(searchpath="./")
    template_env = jinja2.Environment(loader=template_loader)
    template = template_env.get_template("template.html")

    # build Summaries and Graphs
    overall_summary = generate_blocksize_summary(fio_results_df)
    generate_total_throughput_chart(fio_results_df).savefig('images/total_throughput.png')



    # generate the report
    report = template.render(
        overall_summary=overall_summary,
        total_throughput_chart='images/total_throughput.png'
    )

    # write the report to a file
    with open(report_file_path, 'w') as f:
        f.write(report)
   