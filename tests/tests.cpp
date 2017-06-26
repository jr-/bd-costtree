#include "gtest/gtest.h"

#include "OperatorNode.hpp"

using std::pair;

int _block_size = 1024;
int _nBuf = 5;

TEST(TableCreationTest, TableCreation) {
	Table tb("Table", 100);
	EXPECT_EQ("Table", tb.name());
	EXPECT_EQ(100, tb.tuple_quantity());
}

TEST(TrableTest, TableTest) {
	Table tb("Table", 100);
	tb.add_attribute("nome", STRING, 20, 50);
	EXPECT_EQ(100/50, tb.attribute_cardinality("nome"));
	tb.add_attribute("idade", INT, 2, 20);
	EXPECT_EQ(22, tb.size());
	EXPECT_FALSE(tb.has_primary_index());
	tb.add_primary_index(5, 20);
	EXPECT_TRUE(tb.has_primary_index());
}

TEST(ExpressionTest, ExpressionTest) {
	Table tb("Table", 100);
	tb.add_attribute("name", STRING, 20, 50);
	EqualExpression eq = EqualExpression(std::pair<string, string>("Table", "name"), std::pair<string, string>("", "nicolas"));
	EXPECT_EQ(100/50, eq.tuple_quantity(&tb));
	tb.add_attribute("age", INT, 1, 20);
	GreaterExpression gt(std::pair<string, string>("Table", "age"), std::pair<string, string>("", "20"));
	EXPECT_EQ(100/2, gt.tuple_quantity(&tb));
}

TEST(ProjectionOperator, FullTest) {
    int _old_block_size = _block_size;
    _block_size = 1024;

    Table medicos("Medicos", 100);
    medicos.add_attribute("codm", INT, 5, 100);
    medicos.add_attribute("nome", STRING, 15, 100);
    medicos.add_attribute("cidade", STRING, 15, 50);
    medicos.add_attribute("idade", INT, 5, 40);
    medicos.add_attribute("especialidade", STRING, 10, 10);
    EXPECT_EQ(50, medicos.size());

    ProjectionNode projection(&medicos, deque<pair<string, string>>{pair<string, string>("Medicos", "codm"), pair<string, string>("Medicos", "nome")});
    EXPECT_EQ(5, projection.best_access_cost());
    EXPECT_EQ(20, projection.size());
    EXPECT_EQ(100, projection.tuple_quantity());
    EXPECT_EQ(2, projection.block_quantity());
}

TEST(JoinOperator, BestAcessCostTest) {
    int _old_nbuf = _nBuf;
    int _old_block_size = _block_size;
    _nBuf = 5;
    _block_size = 1024;

    Table consultas("Consultas", 1000);
    consultas.add_attribute("codm", INT, 5, 80);
    consultas.add_attribute("codp", INT, 10, 450);
    consultas.add_attribute("data", DATE, 10, 300);
    consultas.add_attribute("hora", INT, 5, 15);

    EXPECT_EQ(30, consultas.size());

    consultas.add_secondary_index("codm", 5, 5);
    consultas.add_secondary_index("codp", 5, 5);
    consultas.add_secondary_index("data", 5, 5);
    consultas.add_secondary_hash_index("codm");
    consultas.add_secondary_hash_index("codp");

    consultas.ordered_by("data");
    consultas.add_foreign_key("codm", "Medicos");
    consultas.add_foreign_key("codp", "Pacientes");

    Table medicos("Medicos", 100);
    medicos.add_attribute("codm", INT, 5, 100);
    medicos.add_attribute("nome", STRING, 15, 100);
    medicos.add_attribute("cidade", STRING, 15, 50);
    medicos.add_attribute("idade", INT, 5, 40);
    medicos.add_attribute("especialidade", STRING, 10, 10);
    EXPECT_EQ(50, medicos.size());

    medicos.add_primary_key(deque<string>{"codm"});
    medicos.add_primary_index(10, 10);
    medicos.add_secondary_hash_index("especialidade");
    medicos.add_secondary_index("cidade", 5, 5);

    medicos.ordered_by("codm");

    EqualExpression eq = EqualExpression(std::pair<string, string>("Medicos", "codm"), std::pair<string, string>("Consultas", "codm"));

    JoinNode join(&medicos, &consultas, &eq);

    EXPECT_EQ(155, join.best_access_cost());//tempo do A1 fazer na mao pros outros
	EXPECT_EQ(1000, join.tuple_quantity()); //juncao por referencia = tuplas da tabela que contem a chave estrangeira
	EXPECT_EQ(75, join.size());


}

TEST(ProductOperator, FullTest) {
    int _old_nbuf = _nBuf;
    int _old_block_size = _block_size;
    _nBuf = 5;
    _block_size = 1024;

    Table consultas("Consultas", 1000);
    consultas.add_attribute("codm", INT, 5, 80);
    consultas.add_attribute("codp", INT, 10, 450);
    consultas.add_attribute("data", DATE, 10, 300);
    consultas.add_attribute("hora", INT, 5, 15);

    EXPECT_EQ(30, consultas.size());

    consultas.add_secondary_index("codm", 5, 5);
    consultas.add_secondary_index("codp", 5, 5);
    consultas.add_secondary_index("data", 5, 5);
    consultas.add_secondary_hash_index("codm");
    consultas.add_secondary_hash_index("codp");

    consultas.ordered_by("data");
    consultas.add_foreign_key("codm", "Medicos");
    consultas.add_foreign_key("codp", "Pacientes");

    Table medicos("Medicos", 100);
    medicos.add_attribute("codm", INT, 5, 100);
    medicos.add_attribute("nome", STRING, 15, 100);
    medicos.add_attribute("cidade", STRING, 15, 50);
    medicos.add_attribute("idade", INT, 5, 40);
    medicos.add_attribute("especialidade", STRING, 10, 10);
    EXPECT_EQ(50, medicos.size());

    medicos.add_primary_key(deque<string>{"codm"});
    medicos.add_primary_index(10, 10);
    medicos.add_secondary_hash_index("especialidade");
    medicos.add_secondary_index("cidade", 5, 5);

    medicos.ordered_by("codm");

    ProductNode product(&medicos, &consultas);

    EXPECT_EQ(155, product.best_access_cost());
	EXPECT_EQ(100000, product.tuple_quantity());
	EXPECT_EQ(80, product.size());
    EXPECT_EQ(8334, product.block_quantity());
}

TEST(FullTest, FullTest) {
	int _old_nbuf = _nBuf;
	int _old_block_size = _block_size;
	_nBuf = 5;
	_block_size = 1024;

	Table consultas("Consultas", 1000);
	consultas.add_attribute("codm", INT, 5, 80);
	consultas.add_attribute("codp", INT, 10, 450);
	consultas.add_attribute("data", DATE, 10, 300);
	consultas.add_attribute("hora", INT, 5, 15);

	EXPECT_EQ(30, consultas.size());

	consultas.add_secondary_index("codm", 5, 5);
	consultas.add_secondary_index("codp", 5, 5);
	consultas.add_secondary_index("data", 5, 5);
	consultas.add_secondary_hash_index("codm");
	consultas.add_secondary_hash_index("codp");

	consultas.ordered_by("data");
	consultas.add_foreign_key("codm", "Medicos");
	consultas.add_foreign_key("codp", "Pacientes");
	//Pacientes will not be used in this example, but will put this for completion sake
	Table medicos("Medicos", 100);
	medicos.add_attribute("codm", INT, 5, 100);
	medicos.add_attribute("nome", STRING, 15, 100);
	medicos.add_attribute("cidade", STRING, 15, 50);
	medicos.add_attribute("idade", INT, 5, 40);
	medicos.add_attribute("especialidade", STRING, 10, 10);
	EXPECT_EQ(50, medicos.size());

	medicos.add_primary_key(deque<string>{"codm"});
	medicos.add_primary_index(10, 10);
	medicos.add_secondary_hash_index("especialidade");
	medicos.add_secondary_index("cidade", 5, 5);

	medicos.ordered_by("codm");

	EqualExpression selectionLeftExpression(std::pair<string, string>("Consultas", "data"), std::pair<string, string>("", "15/10/2007"));
	SelectionNode selection_left(&consultas, &selectionLeftExpression);
	ProjectionNode projection_left(&selection_left, deque<std::pair<string, string>>{std::pair<string, string>("Consultas", "codm")});


	NotEqualExpression not_equal(pair<string, string>("Medicos", "cidade"), pair<string, string>("", "Florianopolis"));
	EqualExpression equal(pair<string, string>("Medicos", "especialidade"), pair<string, string>("", "ortopedista"));
	AndExpression selectionRightExpression(&equal, &not_equal);
	SelectionNode selection_right(&medicos, &selectionRightExpression);
	ProjectionNode projection_right(&selection_right, deque<pair<string, string>>{pair<string, string>("Medicos", "codm"), pair<string, string>("Medicos", "nome")});

	NaturalJoinNode natural_join(&projection_left, &projection_right);

	EXPECT_EQ(5, selection_right.best_access_cost()); //A1
	EXPECT_EQ(10, selection_right.tuple_quantity());
	EXPECT_EQ(500, selection_right.size());

	EXPECT_EQ(1, projection_right.best_access_cost());
	EXPECT_EQ(200, projection_right.size());

	EXPECT_EQ(4, selection_left.best_access_cost()); //A4
	EXPECT_EQ(4, selection_left.tuple_quantity());
	EXPECT_EQ(120, selection_left.size());

	EXPECT_EQ(1, projection_left.best_access_cost());
	EXPECT_EQ(40, projection_left.size());

	EXPECT_EQ(2, natural_join.best_access_cost());
	EXPECT_EQ(4, natural_join.tuple_quantity());
	EXPECT_EQ(80, natural_join.size());

	_nBuf = _old_nbuf;
	_block_size = _old_block_size;
}
