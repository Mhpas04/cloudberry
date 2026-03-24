//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Greenplum, Inc.
//
//	@filename:
//		CPhysicalInnerIndexNLJoin.cpp
//
//	@doc:
//		Implementation of index inner nested-loops join operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalInnerIndexNLJoin.h"

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecNonSingleton.h"
#include "gpopt/base/CDistributionSpecReplicated.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/optimizer/COptimizerConfig.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerIndexNLJoin::CPhysicalInnerIndexNLJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalInnerIndexNLJoin::CPhysicalInnerIndexNLJoin(CMemoryPool *mp,
													 CColRefArray *colref_array,
													 CExpression *origJoinPred)
	: CPhysicalInnerNLJoin(mp),
	  m_pdrgpcrOuterRefs(colref_array),
	  m_origJoinPred(origJoinPred)
{
	GPOS_ASSERT(nullptr != colref_array);
	if (nullptr != origJoinPred)
	{
		origJoinPred->AddRef();
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerIndexNLJoin::~CPhysicalInnerIndexNLJoin
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalInnerIndexNLJoin::~CPhysicalInnerIndexNLJoin()
{
	m_pdrgpcrOuterRefs->Release();
	CRefCount::SafeRelease(m_origJoinPred);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerIndexNLJoin::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CPhysicalInnerIndexNLJoin::Matches(COperator *pop) const
{
	if (pop->Eopid() == Eopid())
	{
		return m_pdrgpcrOuterRefs->Equals(
			CPhysicalInnerIndexNLJoin::PopConvert(pop)->PdrgPcrOuterRefs());
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerIndexNLJoin::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child;
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalInnerIndexNLJoin::PdsRequired(CMemoryPool *mp GPOS_UNUSED,
									   CExpressionHandle &exprhdl GPOS_UNUSED,
									   CDistributionSpec *,	 //pdsRequired,
									   ULONG child_index GPOS_UNUSED,
									   CDrvdPropArray *pdrgpdpCtxt GPOS_UNUSED,
									   ULONG  // ulOptReq
) const
{
	GPOS_RAISE(
		CException::ExmaInvalid, CException::ExmiInvalid,
		GPOS_WSZ_LIT(
			"PdsRequired should not be called for CPhysicalInnerIndexNLJoin"));
	return nullptr;
}

static CColRef* GetColRefByName(const CColRefSet *pcrs, const CWStringBase *pstrName)
{
    CColRefSetIter crsi(*pcrs);
    while (crsi.Advance())
    {
        if (crsi.Pcr()->Name().Pstr()->Equals(pstrName))
        {
            return crsi.Pcr();
        }
    }
    return nullptr;
}

CEnfdDistribution *
CPhysicalInnerIndexNLJoin::Ped(CMemoryPool *mp, CExpressionHandle &exprhdl,
							   CReqdPropPlan *prppInput, ULONG child_index,
							   CDrvdPropArray *pdrgpdpCtxt, ULONG ulDistrReq)
{
	GPOS_ASSERT(2 > child_index);

	CEnfdDistribution::EDistributionMatching dmatch =
		Edm(prppInput, child_index, pdrgpdpCtxt, ulDistrReq);

	COptCtxt *poctxt = COptCtxt::PoctxtFromTLS();
	CPlanHint *planhint = poctxt->GetOptimizerConfig()->GetPlanHint();

	if (planhint != nullptr)
	{
		CTableDescriptorHashSet *tables = exprhdl.DeriveTableDescriptor(child_index);
		CDistributionHint* hint = planhint->GetDistributionHint(tables);
		if (hint != nullptr)
		{
			switch (hint->GetDistributionType())
			{
				case CDistributionHint::BROADCAST:
				{

					CDistributionSpec *pds = GPOS_NEW(mp) CDistributionSpecReplicated(CDistributionSpec::EdtStrictReplicated);
					return GPOS_NEW(mp) CEnfdDistribution(pds, dmatch);

				}
				case CDistributionHint::REDISTRIBUTION:
				{
				    const StringPtrArray* columns = hint->GetColumnNames();
					if(columns != nullptr){
						const auto* outputCols = exprhdl.DeriveOutputColumns(child_index);
                        CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
                        for(ULONG ul = 0; ul < columns->Size(); ++ul){
                            CColRef* pcr = GetColRefByName(outputCols, (*columns)[ul]);
                            CExpression *pexprHashKey = GPOS_NEW(mp) CExpression(
                                mp,
                                GPOS_NEW(mp) CScalarIdent(mp, pcr)
                            );
                            pdrgpexpr->Append(pexprHashKey);
                        }

                        CDistributionSpecHashed *pds = GPOS_NEW(mp) CDistributionSpecHashed(
                            pdrgpexpr,
                            true
                        );

                        return GPOS_NEW(mp) CEnfdDistribution(pds, dmatch);
					}
					return GPOS_NEW(mp) CEnfdDistribution(nullptr, dmatch);
				}
				case CDistributionHint::SINGLENODE:
				{
					CDistributionSpec *pds = GPOS_NEW(mp) CDistributionSpecSingleton(CDistributionSpecSingleton::EstSegment);
					return GPOS_NEW(mp) CEnfdDistribution(pds, dmatch);
				}
				case CDistributionHint::PASSTHROUGH:
				{
					CDistributionSpec *pds = GPOS_NEW(mp) CDistributionSpecAny(this->Eopid());
					return GPOS_NEW(mp) CEnfdDistribution(pds, dmatch);
				}
				case CDistributionHint::SENTINEL:
				{
					break;
				}
			}
		}
	}

	if (1 == child_index)
	{
		// inner (index-scan side) is requested for Any distribution,
		// we allow outer references on the inner child of the join since it needs
		// to refer to columns in join's outer child
		return GPOS_NEW(mp) CEnfdDistribution(
			GPOS_NEW(mp)
				CDistributionSpecAny(this->Eopid(), true /*fAllowOuterRefs*/),
			dmatch);
	}

	// we need to match distribution of inner
	CDistributionSpec *pdsInner =
		CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[0])->Pds();
	CDistributionSpec::EDistributionType edtInner = pdsInner->Edt();
	if (CDistributionSpec::EdtSingleton == edtInner ||
		CDistributionSpec::EdtStrictSingleton == edtInner ||
		CDistributionSpec::EdtUniversal == edtInner)
	{
		// enforce executing on a single host
		return GPOS_NEW(mp) CEnfdDistribution(
			GPOS_NEW(mp) CDistributionSpecSingleton(), dmatch);
	}

	if (CDistributionSpec::EdtHashed == edtInner)
	{
		// check if we could create an equivalent hashed distribution request to the inner child
		CDistributionSpecHashed *pdshashed =
			CDistributionSpecHashed::PdsConvert(pdsInner);
		CDistributionSpecHashed *pdshashedEquiv = pdshashed->PdshashedEquiv();

		// If the inner child is a IndexScan on a multi-key distributed index, it
		// may derive an incomplete equiv spec (see CPhysicalScan::PdsDerive()).
		// However, there is no point to using that here since there will be no
		// operator above this that can complete it.
		if (pdshashed->HasCompleteEquivSpec(mp))
		{
			// request hashed distribution from outer
			pdshashedEquiv->Pdrgpexpr()->AddRef();
			CDistributionSpecHashed *pdsHashedRequired = GPOS_NEW(mp)
				CDistributionSpecHashed(pdshashedEquiv->Pdrgpexpr(),
										pdshashedEquiv->FNullsColocated());
			pdsHashedRequired->ComputeEquivHashExprs(mp, exprhdl);

			return GPOS_NEW(mp) CEnfdDistribution(pdsHashedRequired, dmatch);
		}
	}

	// otherwise, require outer child to be replicated
	// The match type for this request has to be "Satisfy" since EdtReplicated
	// is required only property. Since a Broadcast motion will always
	// derive a EdtStrictReplicated distribution spec, it will never "Match"
	// the required distribution spec and hence will not be optimized.
	return GPOS_NEW(mp) CEnfdDistribution(
		GPOS_NEW(mp)
			CDistributionSpecReplicated(CDistributionSpec::EdtReplicated),
		CEnfdDistribution::EdmSatisfy);
}


// EOF
