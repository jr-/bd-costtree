#include "gtest/gtest.h"

#include "OperatorNode.hpp"

int _block_size = 1024;
int _nBuf = 5;

TEST(TableCreationTest, TableCreation) {
	Table tb("Table", 100);
	EXPECT_EQ("Table", tb.name());
}

/*int main(int argc, char **argv)
{
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}*/
