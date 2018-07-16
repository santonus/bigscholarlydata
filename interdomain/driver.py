import analysis.dataframe as sm
import analysis.cosim as hmap
import topicmodel.generate_topics as tm
import analysis.combined_approach as an
import common.config as c

op={"generate_topics":tm.generate_topics,
	"heat_map":hmap.heatmap,
	"stats_model":sm.create_domain_df,
	"describe_stat_data":sm.desc_stat_data,
	"cosim_boxplots":an.column_analysis,
	"topic_domain_analysis":an.topic_domain_analysis,
	"DocTopic_dist":an.DocTopic_dist,
	"sim_mat_dist":an.sim_mat_dist,
	"common_keywords_analysis":an.common_keywords_analysis,
	"topic_paper_analysis":an.topic_paper_analysis
}

kwargs={
	"cosim_boxplots":{"file":"stats_data/all_data.csv"},
	"describe_stat_data":{"files":["stats_data/rhlcit-pruned.csv","stats_data/rhlpub-pruned.csv"]},
	"sim_mat_dist":{"file":"sim_mat_"+c.query_name},
	"common_keywords_analysis":{"file":"sim_mat_"+c.query_name},
}

def main(operation):
	#op[operation](**kwargs.get(operation,{}))
	operation = "sim_mat_dist"
	op[operation](**kwargs.get(operation,{}))

operation = ""
if __name__ == "__main__": main(operation)
