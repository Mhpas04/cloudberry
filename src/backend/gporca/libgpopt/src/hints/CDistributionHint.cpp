#include "gpopt/hints/CDistributionHint.h"

#include "gpopt/exception.h"
#include "gpopt/hints/CHintUtils.h"
#include "naucrates/dxl/CDXLUtils.h"

using namespace gpopt;

FORCE_GENERATE_DBGSTR(CDistributionHint);

IOstream &
CDistributionHint::OsPrint(IOstream &os) const
{
	os << "Distribution Hint\n";
	return os;
}

CDistributionHint::DistributionType CDistributionHint::GetDistributionType() const
{
	return m_type;
}

const StringPtrArray* CDistributionHint::GetAliasNames() const
{
	return m_aliases;
}

const StringPtrArray* CDistributionHint::GetColumnNames() const
{
	return m_columns;
}
//---------------------------------------------------------------------------
//	@function:
//		CJoinHint::Serialize
//
//	@doc:
//		Serialize the object
//---------------------------------------------------------------------------
void
CDistributionHint::Serialize(CXMLSerializer */*xml_serializer*/)
{
	throw std::runtime_error("Not implemented");
}
