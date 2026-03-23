#ifndef GPOS_CDistributionHint_H
#define GPOS_CDistributionHint_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CRefCount.h"

#include "gpopt/hints/IHint.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/COperator.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

namespace gpopt
{
class CDistributionHint : public IHint, public DbgPrintMixin<CDistributionHint>
{
public:
	enum DistributionType
	{
		BROADCAST,
		REDISTRIBUTION,
		SINGLENODE,
        PASSTHROUGH,
		SENTINEL
	};
private:
	CMemoryPool *m_mp;
	DistributionType m_type{SENTINEL};
	// sorted list of alias names.
	StringPtrArray *m_aliases{nullptr};
    // List of Column names. Used for columns to repartition on
    StringPtrArray *m_columns{nullptr};
public:
	explicit CDistributionHint(CMemoryPool *mp, DistributionType type, StringPtrArray *aliases, StringPtrArray* columns) : m_mp(mp), m_type(type), m_aliases(aliases), m_columns(columns) {
		m_aliases->Sort(CWStringBase::Compare);
	}

	~CDistributionHint() override
	{
		CRefCount::SafeRelease(m_aliases);
	}

	IOstream &OsPrint(IOstream &os) const;

	DistributionType GetDistributionType() const;

	const StringPtrArray *GetAliasNames() const;

    const StringPtrArray *GetColumnNames() const;

	void Serialize(CXMLSerializer *xml_serializer);
};

using DistributionHintList = CDynamicPtrArray<CDistributionHint, CleanupRelease>;

}  // namespace gpopt

#endif	// !GPOS_CDistributionHint_H

// EOF
